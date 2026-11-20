package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

func TestPendingErrorIdentityEnrichAndResolve(t *testing.T) {
	submitted := time.Unix(100, 0)
	for _, operation := range []string{"enrich", "resolve"} {
		for _, shape := range []string{"all_missing", "custom_present", "model_present", "submitted_present"} {
			t.Run(operation+"/"+shape, func(t *testing.T) {
				pending := NewPendingRequests(1)
				req := RequestItem{RequestID: "r1", CustomID: "c1", ModelID: "m1", SubmittedAt: submitted}
				pending.Store(req)
				originalError := &OutputError{Code: "DEADLINE_EXCEEDED", Message: "retained failure"}
				result := ResultItem{RequestID: req.RequestID, Error: originalError}
				switch shape {
				case "custom_present":
					result.CustomID = req.CustomID
				case "model_present":
					result.ModelID = req.ModelID
				case "submitted_present":
					result.SubmittedAt = req.SubmittedAt
				}
				accepted := false
				if operation == "enrich" {
					accepted = pending.Enrich(&result)
				} else {
					accepted = pending.Resolve(&result)
				}
				if !accepted {
					t.Fatal("own error rejected")
				}
				if result.CustomID != req.CustomID || result.ModelID != req.ModelID || result.SubmittedAt != req.SubmittedAt {
					t.Errorf("ERROR_IDENTITY_METADATA_MISSING: %s %s got=%+v", operation, shape, result)
				}
				if result.Error != originalError || result.RequestID != req.RequestID {
					t.Fatal("error or request identity changed")
				}
				if operation == "enrich" && !pending.Has(req.RequestID) {
					t.Fatal("enrichment resolved before checkpoint")
				}
				if operation == "enrich" && !pending.Resolve(&result) {
					t.Fatal("resolve after enrichment rejected")
				}
				if pending.Has(req.RequestID) {
					t.Fatal("successful resolve left pending")
				}
				if pending.Resolve(&ResultItem{RequestID: req.RequestID, Error: originalError}) {
					t.Fatal("metadata-free duplicate accepted")
				}
				if pending.Enrich(&ResultItem{RequestID: "foreign", Error: originalError}) {
					t.Fatal("foreign error accepted")
				}
				inline := ResultItem{RequestID: "inline", CustomID: "inline-custom", ModelID: "inline-model", SubmittedAt: submitted, Error: originalError}
				if !pending.Enrich(&inline) || !pending.Resolve(&inline) || inline.CustomID != "inline-custom" || inline.ModelID != "inline-model" || inline.SubmittedAt != submitted {
					t.Fatal("inline error semantics changed")
				}
			})
		}
	}
}

func TestPendingErrorIdentityDurableMixedJobs(t *testing.T) {
	submitted := time.Unix(100, 0)
	responses := []inference.GenerateResponse{
		{StatusCode: 200, Response: []byte(`{"choices":[]}`)},
		{ErrorCode: "DEADLINE_EXCEEDED", ErrorMessage: "retained deadline failure"},
		{StatusCode: 200},
		{StatusCode: 200, Response: []byte(`{invalid`)},
		{StatusCode: 403},
		{StatusCode: 500, Response: []byte(`backend unavailable`)},
	}
	for _, job := range []string{"a", "b"} {
		t.Run(job, func(t *testing.T) {
			pending := NewPendingRequests(6)
			for i := range responses {
				pending.Store(RequestItem{RequestID: fmt.Sprintf("%s-r%d", job, i), CustomID: fmt.Sprintf("%s-c%d", job, i), ModelID: "model-" + job, SubmittedAt: submitted})
			}
			sink := &fakeSink{}
			sink.fn = func(msg *ResultItem) (bool, error) {
				if !pending.Has(msg.RequestID) {
					t.Error("checkpoint after pending ownership release")
				}
				return true, nil
			}
			tracker := NewProgressTracker(6, nil, "job-"+job, 0, logr.Discard())
			checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())
			ch := make(chan ResultItem, 13)
			order := []int{4, 1, 5, 0, 3, 2}
			for _, i := range order {
				for _, owner := range []string{"b", "a"} {
					response := responses[i]
					response.RequestID = fmt.Sprintf("%s-r%d", owner, i)
					ch <- asyncResult(&response, logr.Discard())
				}
			}
			ch <- asyncResult(&inference.GenerateResponse{RequestID: "unknown", ErrorCode: "GATE_DROPPED", ErrorMessage: "not owned"}, logr.Discard())
			close(ch)
			if err := checkpointer.Drain(context.Background(), ch); err != nil {
				t.Fatal(err)
			}
			records := sink.recorded()
			if len(records) != 6 {
				t.Fatalf("checkpoint count=%d want6", len(records))
			}
			seen := map[string]bool{}
			for _, record := range records {
				var line outputLine
				if err := json.Unmarshal(record.line, &line); err != nil {
					t.Fatal(err)
				}
				var i int
				if _, err := fmt.Sscanf(line.ID, job+"-r%d", &i); err != nil || i < 0 || i >= 6 {
					t.Fatalf("foreign requestID %q", line.ID)
				}
				expectedCustom := fmt.Sprintf("%s-c%d", job, i)
				if line.CustomID != expectedCustom || seen[line.CustomID] || record.msg.ModelID != "model-"+job || record.msg.SubmittedAt != submitted {
					t.Errorf("ERROR_IDENTITY_METADATA_MISSING: durable line=%+v msg=%+v", line, record.msg)
				}
				seen[line.CustomID] = true
				expected := responses[i]
				expected.RequestID = line.ID
				want := asyncResult(&expected, logr.Discard())
				gotPayload, _ := json.Marshal(struct {
					Response any
					Error    any
				}{line.Response, line.Error})
				wantPayload, _ := json.Marshal(struct {
					Response any
					Error    any
				}{want.Response, want.Error})
				if string(gotPayload) != string(wantPayload) {
					t.Errorf("error/status/body changed for %s", line.ID)
				}
			}
			if len(pending.IDs()) != 0 {
				t.Fatal("pending remains after checkpoint")
			}
			counts := tracker.Counts()
			if counts.Completed != 1 || counts.Failed != 5 {
				t.Fatalf("counts=%+v", counts)
			}
		})
	}
}

func TestPendingErrorIdentityCheckpointFailure(t *testing.T) {
	pending := NewPendingRequests(1)
	pending.Store(RequestItem{RequestID: "r", CustomID: "c", ModelID: "m", SubmittedAt: time.Unix(100, 0)})
	failure := errors.New("retained checkpoint failure")
	sink := &fakeSink{fn: func(msg *ResultItem) (bool, error) {
		if !pending.Has(msg.RequestID) {
			t.Error("ownership released before checkpoint")
		}
		return false, failure
	}}
	c := NewResultCheckpointer(sink, pending, NewProgressTracker(1, nil, "job", 0, logr.Discard()), logr.Discard())
	acked := false
	msg := asyncResult(&inference.GenerateResponse{RequestID: "r", ErrorCode: "DEADLINE_EXCEEDED", ErrorMessage: "retained"}, logr.Discard())
	msg.Ack = func(context.Context) error { acked = true; return nil }
	ch := make(chan ResultItem, 1)
	ch <- msg
	close(ch)
	if err := c.Drain(context.Background(), ch); !errors.Is(err, failure) {
		t.Fatalf("checkpoint error=%v", err)
	}
	if !pending.Has("r") || acked || sink.len() != 0 {
		t.Fatal("failed checkpoint lost ownership or acknowledged delivery")
	}
}
