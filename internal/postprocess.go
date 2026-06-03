package internal

import (
	"sync"

	"github.com/nexus-dl/afd/pkg/config"
)

type PostProcessor struct {
	config   *config.PostProcessConfig
	mu       sync.RWMutex
	registry map[string]PostProcessorFunc
}

type PostProcessorFunc func(src, dst string) error

func NewPostProcessor(cfg *config.PostProcessConfig) *PostProcessor {
	if cfg == nil {
		cfg = config.DefaultPostProcessConfig()
	}
	return &PostProcessor{
		config:   cfg,
		registry: make(map[string]PostProcessorFunc),
	}
}

func (p *PostProcessor) RegisterProcessor(name string, fn PostProcessorFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.registry[name] = fn
}

func (p *PostProcessor) GetProcessor(name string) (PostProcessorFunc, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	fn, ok := p.registry[name]
	return fn, ok
}

func (p *PostProcessor) ExtractArchive(filePath, outputDir string) error {
	if !p.config.Extract.Enabled {
		return nil
	}
	// 完整的解压功能将在后续版本中实现，现在跳过
	return nil
}

func (p *PostProcessor) Process(filePath string) error {
	// 暂时简化实现
	return nil
}

func (p *PostProcessor) ProcessWithExtract(filePath string) error {
	// 暂时简化实现
	return nil
}

func (p *PostProcessor) SetConfig(cfg *config.PostProcessConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.config = cfg
}

func (p *PostProcessor) GetConfig() *config.PostProcessConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config
}


