package downloader

import "github.com/nexus-dl/afd/pkg/config"

type ChunkStatus int

const (
	ChunkPending    ChunkStatus = iota
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

func SplitFileIntoChunks(fileSize int64, cfg *config.DownloadConfig) []*Chunk {
	if fileSize <= 0 {
		return []*Chunk{{Start: 0, End: 0, Status: ChunkPending}}
	}

	chunkSize := cfg.DefaultChunkSize
	if chunkSize < cfg.MinChunkSize {
		chunkSize = cfg.MinChunkSize
	}
	if chunkSize > cfg.MaxChunkSize {
		chunkSize = cfg.MaxChunkSize
	}

	numChunks := int(fileSize / chunkSize)
	if fileSize%chunkSize != 0 {
		numChunks++
	}

	if cfg.MaxConnections > 0 && numChunks < cfg.MaxConnections && chunkSize > cfg.MinChunkSize {
		adjustedSize := fileSize / int64(cfg.MaxConnections)
		if adjustedSize >= cfg.MinChunkSize {
			chunkSize = adjustedSize
			numChunks = int(fileSize / chunkSize)
			if fileSize%chunkSize != 0 {
				numChunks++
			}
		}
	}

	if numChunks > cfg.MaxConnections && cfg.MaxConnections > 0 {
		numChunks = cfg.MaxConnections
		chunkSize = fileSize / int64(numChunks)
	}

	chunks := make([]*Chunk, 0, numChunks)

	for i := 0; i < numChunks; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1
		if i == numChunks-1 {
			end = fileSize - 1
		}

		if start < 0 {
			start = 0
		}
		if end >= fileSize {
			end = fileSize - 1
		}
		if start > end {
			continue
		}

		chunks = append(chunks, &Chunk{
			Start:  start,
			End:    end,
			Status: ChunkPending,
		})
	}

	return chunks
}