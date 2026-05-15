package mm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	yaml := `
server_url: "http://localhost:9090"
polygon_api_key: "test-key"
price_source: polygon

symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    spread: 10000
    quantity: 10
    levels: 3
    max_position: 100
    requote_interval: 15s
    price_move_threshold: 5000

polygon:
  base_url: "https://api.example.com"
  poll_interval: 60s
`
	path := writeTestConfig(t, yaml)
	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	assert.Equal(t, "http://localhost:9090", cfg.ServerURL)
	assert.Equal(t, "test-key", cfg.PolygonKey)
	assert.Equal(t, "polygon", cfg.PriceSource)
	assert.Equal(t, "https://api.example.com", cfg.Polygon.BaseURL)
	assert.Equal(t, 60*time.Second, cfg.Polygon.PollInterval)

	require.Len(t, cfg.Symbols, 1)
	s := cfg.Symbols[0]
	assert.Equal(t, "AAPL", s.Symbol)
	assert.Equal(t, "mm-AAPL", s.AccountID)
	assert.Equal(t, int64(10000), s.Spread)
	assert.Equal(t, int64(10), s.Quantity)
	assert.Equal(t, 3, s.Levels)
	assert.Equal(t, int64(10000), s.LevelSpacing) // defaults to spread
	assert.Equal(t, int64(100), s.MaxPosition)
	assert.Equal(t, 15*time.Second, s.RequoteInterval)
	assert.Equal(t, int64(5000), s.PriceMoveThreshold)
}

func TestLoadConfig_Defaults(t *testing.T) {
	yaml := `
polygon_api_key: "key"

symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    spread: 10000
    quantity: 10
    max_position: 100
`
	path := writeTestConfig(t, yaml)
	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	assert.Equal(t, "http://localhost:8080", cfg.ServerURL)
	assert.Equal(t, "polygon", cfg.PriceSource)
	assert.Equal(t, "https://api.polygon.io", cfg.Polygon.BaseURL)
	assert.Equal(t, 30*time.Second, cfg.Polygon.PollInterval)
	assert.Equal(t, 1, cfg.Symbols[0].Levels)
	assert.Equal(t, int64(10000), cfg.Symbols[0].LevelSpacing)
	assert.Equal(t, 30*time.Second, cfg.Symbols[0].RequoteInterval)
}

func TestLoadConfig_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "no symbols",
			yaml: `polygon_api_key: "key"`,
			want: "at least one symbol",
		},
		{
			name: "unknown price source",
			yaml: `
price_source: unknown
symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    spread: 10000
    quantity: 10
    max_position: 100
`,
			want: "unknown price_source",
		},
		{
			name: "polygon without key",
			yaml: `
price_source: polygon
symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    spread: 10000
    quantity: 10
    max_position: 100
`,
			want: "polygon_api_key is required",
		},
		{
			name: "missing symbol name",
			yaml: `
polygon_api_key: "key"
symbols:
  - account_id: mm-AAPL
    spread: 10000
    quantity: 10
    max_position: 100
`,
			want: "symbol is required",
		},
		{
			name: "missing account_id",
			yaml: `
polygon_api_key: "key"
symbols:
  - symbol: AAPL
    spread: 10000
    quantity: 10
    max_position: 100
`,
			want: "account_id is required",
		},
		{
			name: "zero spread",
			yaml: `
polygon_api_key: "key"
symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    quantity: 10
    max_position: 100
`,
			want: "spread must be positive",
		},
		{
			name: "static without price",
			yaml: `
price_source: static
symbols:
  - symbol: AAPL
    account_id: mm-AAPL
    spread: 10000
    quantity: 10
    max_position: 100
`,
			want: "no static price",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTestConfig(t, tt.yaml)
			_, err := LoadConfig(path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	return path
}
