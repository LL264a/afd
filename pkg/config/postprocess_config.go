package config

type ExtractConfig struct {
	Enabled            bool   `json:"enabled"`
	OutputDir          string `json:"output_dir"`
	DeleteAfterExtract bool   `json:"delete_after_extract"`
	Overwrite          bool   `json:"overwrite"`
	Password           string `json:"password,omitempty"`
	StripComponents    int    `json:"strip_components"`
}

type MoveConfig struct {
	Enabled      bool   `json:"enabled"`
	Destination  string `json:"destination"`
	Overwrite    bool   `json:"overwrite"`
	DeleteSource bool   `json:"delete_source"`
}

type CleanupConfig struct {
	Enabled          bool     `json:"enabled"`
	DeleteSourceFile bool     `json:"delete_source_file"`
	DeleteTempFiles  bool     `json:"delete_temp_files"`
	Patterns         []string `json:"patterns"`
}

type PostProcessConfig struct {
	Extract ExtractConfig `json:"extract"`
	Move    MoveConfig    `json:"move"`
	Cleanup CleanupConfig `json:"cleanup"`
}

func DefaultExtractConfig() *ExtractConfig {
	return &ExtractConfig{
		Enabled:            false,
		OutputDir:          "",
		DeleteAfterExtract: false,
		Overwrite:          false,
		StripComponents:    0,
	}
}

func DefaultMoveConfig() *MoveConfig {
	return &MoveConfig{
		Enabled:      false,
		Destination:  "",
		Overwrite:    false,
		DeleteSource: false,
	}
}

func DefaultCleanupConfig() *CleanupConfig {
	return &CleanupConfig{
		Enabled:          false,
		DeleteSourceFile: false,
		DeleteTempFiles:  false,
		Patterns:         []string{},
	}
}

func DefaultPostProcessConfig() *PostProcessConfig {
	return &PostProcessConfig{
		Extract: *DefaultExtractConfig(),
		Move:    *DefaultMoveConfig(),
		Cleanup: *DefaultCleanupConfig(),
	}
}
