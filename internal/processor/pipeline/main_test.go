package pipeline

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
)

func TestMain(m *testing.M) {
	cfg := config.NewConfig()
	if err := metrics.InitMetrics(*cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to init metrics for pipeline tests: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func makeItems(n int, model string) []RequestItem {
	items := make([]RequestItem, n)
	for i := range items {
		items[i] = RequestItem{
			RequestID: fmt.Sprintf("req-%s-%d", model, i),
			CustomID:  fmt.Sprintf("c-%d", i),
			ModelID:   model,
			Endpoint:  "/v1/chat/completions",
		}
	}
	return items
}

func countLines(data []byte) int {
	return len(splitLines(data))
}

func splitLines(data []byte) [][]byte {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil
	}
	return bytes.Split(trimmed, []byte("\n"))
}
