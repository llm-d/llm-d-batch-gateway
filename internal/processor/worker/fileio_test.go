package worker

import (
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
)

func TestJobRootDir_EmptyTenantID_ReturnsError(t *testing.T) {
	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{})

	if _, err := p.jobRootDir("job-1", ""); err == nil {
		t.Fatalf("expected error for empty tenantID")
	}
}
