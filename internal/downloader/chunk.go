package downloader

import (
	"encoding/base64"
	"sync"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
)

// Block 是 Piece 内的最小下载单元，由 BitfieldMan 位图追踪完成状态
const (
	DefaultBlockLength int64 = 256 * 1024 // 256KB per block
)

// PieceStatus 追踪 Piece 的下载状态
type PieceStatus int

const (
	PieceIdle PieceStatus = iota
	PieceActive
	PieceComplete
)

// Piece 对应文件的一个区间，内部由 Block 位图追踪
type Piece struct {
	Index    int
	Start    int64 // 文件内绝对偏移
	Length   int64 // Piece 总长度
	status   PieceStatus
	blocks   *BitfieldMan
	mu       sync.Mutex
	ownerCUID int64 // 当前下载此 Piece 的 goroutine ID（用于 segment stealing）
}

func NewPiece(index int, start, length, blockLength int64) *Piece {
	numBlocks := int(length / blockLength)
	if length%blockLength != 0 {
		numBlocks++
	}
	if numBlocks == 0 {
		numBlocks = 1
	}
	return &Piece{
		Index:  index,
		Start:  start,
		Length: length,
		status: PieceIdle,
		blocks: NewBitfieldMan(numBlocks, blockLength, length),
	}
}

// NextUnusedBlock 原子地获取一个未下载且未被占用的 block，返回其在文件内的绝对偏移和长度
// 如果没有可用 block，返回 -1, 0
func (p *Piece) NextUnusedBlock() (offset int64, length int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	idx := p.blocks.FirstMissingUnusedIndex()
	if idx < 0 {
		return -1, 0
	}
	p.blocks.SetUseBit(idx)
	blockLen := p.blocks.BlockLength(idx)
	return p.Start + int64(idx)*p.blocks.blockLength, blockLen
}

// CompleteBlock 标记一个 block 为已完成
func (p *Piece) CompleteBlock(blockIndex int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocks.SetBit(blockIndex)
	p.blocks.UnsetUseBit(blockIndex)
	if p.blocks.IsAllBitSet() {
		p.status = PieceComplete
	}
}

// CancelBlock 取消一个 block 的占用（用于重试或 segment stealing）
func (p *Piece) CancelBlock(blockIndex int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocks.UnsetUseBit(blockIndex)
}

// IsComplete 返回 Piece 是否所有 block 都已完成
func (p *Piece) IsComplete() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blocks.IsAllBitSet()
}

// CompletedLength 返回已完成的字节数
func (p *Piece) CompletedLength() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blocks.CompletedLength()
}

// RemainingLength 返回剩余未完成的字节数
func (p *Piece) RemainingLength() int64 {
	return p.Length - p.CompletedLength()
}

// SetOwner 设置当前下载此 Piece 的 goroutine
func (p *Piece) SetOwner(cuid int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ownerCUID = cuid
}

// GetOwner 获取当前下载此 Piece 的 goroutine
func (p *Piece) GetOwner() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ownerCUID
}

// StealableRange 尝试从一个慢速 Piece 中"偷"走后半段未下载的范围
// 返回偷到的起始偏移和长度；如果不可偷则返回 -1, 0
func (p *Piece) StealableRange(minSize int64) (offset int64, length int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	remaining := p.blocks.RemainingLength()
	if remaining < minSize {
		return -1, 0
	}

	// 找到第一个未完成的 block，从中间位置开始偷
	firstMissing := p.blocks.FirstMissingIndex()
	if firstMissing < 0 {
		return -1, 0
	}

	// 从第一个未完成 block 的中间开始偷
	stealStartBlock := firstMissing + p.blocks.CountMissing()/2
	if stealStartBlock <= firstMissing {
		stealStartBlock = firstMissing
	}

	// 只释放未完成 block 的 useBit，已完成的 block 不受影响，
	// 这样 stealer 可以获取未完成范围而不会破坏已下载数据。
	for i := stealStartBlock; i < p.blocks.NumBlocks(); i++ {
		if !p.blocks.IsBitSet(i) {
			p.blocks.UnsetUseBit(i)
		}
	}

	offset = p.Start + int64(stealStartBlock)*p.blocks.blockLength
	length = p.Length - (int64(stealStartBlock) * p.blocks.blockLength)
	if length < minSize {
		return -1, 0
	}

	return offset, length
}

// BlockIndexForOffset 根据文件偏移量返回 block 索引
func (p *Piece) BlockIndexForOffset(offset int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	relOffset := offset - p.Start
	idx := int(relOffset / p.blocks.blockLength)
	if idx >= p.blocks.NumBlocks() {
		idx = p.blocks.NumBlocks() - 1
	}
	return idx
}

// Status 返回 Piece 状态
func (p *Piece) Status() PieceStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status
}

// SetStatus 设置 Piece 状态
func (p *Piece) SetStatus(s PieceStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = s
}

// BitfieldMan 位图管理器，追踪 Block 级完成状态
type BitfieldMan struct {
	numBlocks       int
	blockLength     int64
	totalLength     int64
	bitfield        []byte // 完成位图
	useBitfield     []byte // 占用位图（防止多个 goroutine 下载同一个 block）
	completedCount  int    // 已完成的 block 数量（O(1) 判断是否全部完成）
}

func NewBitfieldMan(numBlocks int, blockLength, totalLength int64) *BitfieldMan {
	bfLen := (numBlocks + 7) / 8
	return &BitfieldMan{
		numBlocks:   numBlocks,
		blockLength: blockLength,
		totalLength: totalLength,
		bitfield:    make([]byte, bfLen),
		useBitfield: make([]byte, bfLen),
	}
}

func (b *BitfieldMan) NumBlocks() int      { return b.numBlocks }
func (b *BitfieldMan) BlockLength(idx int) int64 {
	if idx == b.numBlocks-1 {
		// 最后一个 block 可能较短
		rem := b.totalLength % b.blockLength
		if rem != 0 {
			return rem
		}
	}
	return b.blockLength
}

// SetBit 标记 block 为已完成
func (b *BitfieldMan) SetBit(idx int) {
	if idx < 0 || idx >= b.numBlocks {
		return
	}
	byteIdx := idx / 8
	bitIdx := uint(idx) % 8
	if b.bitfield[byteIdx]&(1<<bitIdx) == 0 { // 原先未设置，递增计数器
		b.completedCount++
	}
	b.bitfield[byteIdx] |= 1 << bitIdx
}

// ClearBit 清除 block 的完成标记
func (b *BitfieldMan) ClearBit(idx int) {
	if idx < 0 || idx >= b.numBlocks {
		return
	}
	byteIdx := idx / 8
	bitIdx := uint(idx) % 8
	if b.bitfield[byteIdx]&(1<<bitIdx) != 0 { // 原先已设置，递减计数器
		b.completedCount--
	}
	b.bitfield[byteIdx] &^= 1 << bitIdx
}

// UnsetUseBit 释放 block 的占用
func (b *BitfieldMan) UnsetUseBit(idx int) {
	if idx >= 0 && idx < b.numBlocks {
		b.useBitfield[idx/8] &^= 1 << (uint(idx) % 8)
	}
}

// SetUseBit 标记 block 为占用中
func (b *BitfieldMan) SetUseBit(idx int) {
	if idx >= 0 && idx < b.numBlocks {
		b.useBitfield[idx/8] |= 1 << (uint(idx) % 8)
	}
}

// IsBitSet 检查 block 是否已完成
func (b *BitfieldMan) IsBitSet(idx int) bool {
	if idx < 0 || idx >= b.numBlocks {
		return false
	}
	return b.bitfield[idx/8]&(1<<(uint(idx)%8)) != 0
}

// IsUseBitSet 检查 block 是否被占用
func (b *BitfieldMan) IsUseBitSet(idx int) bool {
	if idx < 0 || idx >= b.numBlocks {
		return false
	}
	return b.useBitfield[idx/8]&(1<<(uint(idx)%8)) != 0
}

// IsAllBitSet 检查是否所有 block 都已完成
func (b *BitfieldMan) IsAllBitSet() bool {
	return b.completedCount == b.numBlocks
}

// FirstMissingUnusedIndex 找到第一个未完成且未被占用的 block
func (b *BitfieldMan) FirstMissingUnusedIndex() int {
	for i := 0; i < b.numBlocks; i++ {
		if !b.IsBitSet(i) && !b.IsUseBitSet(i) {
			return i
		}
	}
	return -1
}

// FirstMissingIndex 找到第一个未完成的 block
func (b *BitfieldMan) FirstMissingIndex() int {
	for i := 0; i < b.numBlocks; i++ {
		if !b.IsBitSet(i) {
			return i
		}
	}
	return -1
}

// CountMissing 返回未完成的 block 数量
func (b *BitfieldMan) CountMissing() int {
	count := 0
	for i := 0; i < b.numBlocks; i++ {
		if !b.IsBitSet(i) {
			count++
		}
	}
	return count
}

// CompletedLength 返回已完成的字节数
func (b *BitfieldMan) CompletedLength() int64 {
	var total int64
	for i := 0; i < b.numBlocks; i++ {
		if b.IsBitSet(i) {
			total += b.BlockLength(i)
		}
	}
	return total
}

// RemainingLength 返回未完成的字节数
func (b *BitfieldMan) RemainingLength() int64 {
	return b.totalLength - b.CompletedLength()
}

// GetBitfield 返回完成位图的副本（用于进度保存）
func (b *BitfieldMan) GetBitfield() []byte {
	cp := make([]byte, len(b.bitfield))
	copy(cp, b.bitfield)
	return cp
}

// SetBitfield 从保存的位图恢复状态
func (b *BitfieldMan) SetBitfield(bf []byte) {
	if len(bf) != len(b.bitfield) {
		return
	}
	copy(b.bitfield, bf)
	// 重新计算已完成 block 数量
	b.completedCount = 0
	for i := 0; i < b.numBlocks; i++ {
		if b.IsBitSet(i) {
			b.completedCount++
		}
	}
}

// SplitFileIntoPieces 将文件分割为 Pieces
func SplitFileIntoPieces(fileSize int64, cfg *config.DownloadConfig) []*Piece {
	if fileSize <= 0 {
		return []*Piece{NewPiece(0, 0, 0, DefaultBlockLength)}
	}

	// Piece 大小 = min-split-size（默认 20MB，类似 aria2）
	pieceSize := cfg.DefaultChunkSize
	if pieceSize < cfg.MinChunkSize {
		pieceSize = cfg.MinChunkSize
	}
	if pieceSize > cfg.MaxChunkSize {
		pieceSize = cfg.MaxChunkSize
	}

	numPieces := int(fileSize / pieceSize)
	if fileSize%pieceSize != 0 {
		numPieces++
	}

	// 如果 Piece 数量少于连接数，减小 Piece 大小
	if cfg.MaxConnections > 0 && numPieces < cfg.MaxConnections && pieceSize > cfg.MinChunkSize {
		adjustedSize := fileSize / int64(cfg.MaxConnections)
		if adjustedSize >= cfg.MinChunkSize {
			pieceSize = adjustedSize
			numPieces = int(fileSize / pieceSize)
			if fileSize%pieceSize != 0 {
				numPieces++
			}
		}
	}

	blockLength := DefaultBlockLength

	pieces := make([]*Piece, 0, numPieces)
	for i := 0; i < numPieces; i++ {
		start := int64(i) * pieceSize
		length := pieceSize
		if start+length > fileSize {
			length = fileSize - start
		}
		pieces = append(pieces, NewPiece(i, start, length, blockLength))
	}

	return pieces
}

// PieceManager 管理所有 Pieces，提供线程安全的 Piece 分配和 Segment Stealing
type PieceManager struct {
	pieces    []*Piece
	mu        sync.Mutex
	fileSize  int64
	cuidSeq   int64
}

func NewPieceManager(pieces []*Piece, fileSize int64) *PieceManager {
	return &PieceManager{
		pieces:   pieces,
		fileSize: fileSize,
	}
}

// NextCUID 生成唯一的 goroutine ID
func (pm *PieceManager) NextCUID() int64 {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.cuidSeq++
	return pm.cuidSeq
}

// GetPieceForDownload 获取一个需要下载的 Piece
// 返回 Piece 和是否成功获取
func (pm *PieceManager) GetPieceForDownload(cuid int64) *Piece {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, p := range pm.pieces {
		if p.Status() == PieceIdle {
			p.SetStatus(PieceActive)
			p.SetOwner(cuid)
			return p
		}
	}
	return nil
}

// TryStealPiece 尝试从一个慢速活跃 Piece 中偷走后半段
// 返回偷到的起始偏移和结束偏移，以及源 Piece
func (pm *PieceManager) TryStealPiece(cuid int64, minStealSize int64) (start int64, end int64, piece *Piece) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, p := range pm.pieces {
		if p.Status() == PieceActive && p.GetOwner() != cuid {
			offset, length := p.StealableRange(minStealSize)
			if offset >= 0 && length >= minStealSize {
				return offset, offset + length - 1, p
			}
		}
	}
	return -1, -1, nil
}

// CompletePiece 标记一个 Piece 为完成
func (pm *PieceManager) CompletePiece(index int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if index >= 0 && index < len(pm.pieces) {
		pm.pieces[index].SetStatus(PieceComplete)
	}
}

// AllComplete 检查是否所有 Piece 都已完成
func (pm *PieceManager) AllComplete() bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, p := range pm.pieces {
		if !p.IsComplete() {
			return false
		}
	}
	return true
}

// TotalCompletedLength 返回所有 Piece 已完成的总字节数
func (pm *PieceManager) TotalCompletedLength() int64 {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	var total int64
	for _, p := range pm.pieces {
		total += p.CompletedLength()
	}
	return total
}

// GetEndOffset 根据 aria2 的逻辑，计算当前 segment 的 Range 结束位置
// 如果下一个 Piece 已经被其他连接下载，则截止到当前 Piece 末尾
// 如果下一个 Piece 无人下载，则可以扩展到文件末尾
func (pm *PieceManager) GetEndOffset(pieceIndex int) int64 {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 检查下一个 Piece 是否已被占用
	nextIdx := pieceIndex + 1
	if nextIdx < len(pm.pieces) && pm.pieces[nextIdx].Status() == PieceActive {
		// 下一个 Piece 有人下载，截止到当前 Piece 末尾
		p := pm.pieces[pieceIndex]
		return p.Start + p.Length - 1
	}

	// 下一个 Piece 无人下载，可以读到文件末尾
	return pm.fileSize - 1
}

// GetPieceByIndex 根据 index 获取 Piece
func (pm *PieceManager) GetPieceByIndex(index int) *Piece {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if index >= 0 && index < len(pm.pieces) {
		return pm.pieces[index]
	}
	return nil
}

// PieceCount 返回 Piece 总数
func (pm *PieceManager) PieceCount() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.pieces)
}

// ActivePieceCount 返回活跃的 Piece 数量
func (pm *PieceManager) ActivePieceCount() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	count := 0
	for _, p := range pm.pieces {
		if p.Status() == PieceActive {
			count++
		}
	}
	return count
}

// SerializePieceBitfields 将所有 Piece 的 Block 完成位图序列化为可存储的格式
func (pm *PieceManager) SerializePieceBitfields() []task.PieceBitfieldEntry {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	entries := make([]task.PieceBitfieldEntry, 0, len(pm.pieces))
	for _, p := range pm.pieces {
		p.mu.Lock()
		bf := p.blocks.GetBitfield()
		p.mu.Unlock()
		// 只有序列化有意义的位图（非空且有已完成 block）
		hasCompleted := false
		for _, b := range bf {
			if b != 0 {
				hasCompleted = true
				break
			}
		}
		if hasCompleted {
			entries = append(entries, task.PieceBitfieldEntry{
				Index:    p.Index,
				Bitfield: base64.StdEncoding.EncodeToString(bf),
			})
		}
	}
	return entries
}

// RestorePieceBitfields 从保存的位图恢复每个 Piece 的 Block 完成状态
func (pm *PieceManager) RestorePieceBitfields(entries []task.PieceBitfieldEntry) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, entry := range entries {
		if entry.Index < 0 || entry.Index >= len(pm.pieces) {
			continue
		}
		bf, err := base64.StdEncoding.DecodeString(entry.Bitfield)
		if err != nil {
			continue
		}
		p := pm.pieces[entry.Index]
		p.mu.Lock()
		p.blocks.SetBitfield(bf)
		// 检查是否所有 block 都完成了
		if p.blocks.IsAllBitSet() {
			p.status = PieceComplete
		}
		p.mu.Unlock()
	}
}


// 保留旧的 Chunk 类型以兼容 singleThreadDownload
type ChunkStatus int

const (
	ChunkPending ChunkStatus = iota
	ChunkDownloading
	ChunkDone
	ChunkFailed
)

type Chunk struct {
	Start      int64
	End        int64
	Downloaded int64
	Status     ChunkStatus
}

func (c *Chunk) Size() int64 {
	return c.End - c.Start + 1
}

func (c *Chunk) Remaining() int64 {
	return c.Size() - c.Downloaded
}

// SplitFileIntoChunks 保留兼容
func SplitFileIntoChunks(fileSize int64, cfg *config.DownloadConfig) []*Chunk {
	pieces := SplitFileIntoPieces(fileSize, cfg)
	chunks := make([]*Chunk, len(pieces))
	for i, p := range pieces {
		chunks[i] = &Chunk{
			Start:  p.Start,
			End:    p.Start + p.Length - 1,
			Status: ChunkPending,
		}
	}
	return chunks
}
