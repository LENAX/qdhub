package jobs

import "testing"

func TestEffectiveSubtaskBatchSize(t *testing.T) {
	t.Parallel()
	if g := effectiveSubtaskBatchSize(200, syncMemNormal); g != 200 {
		t.Errorf("normal: got %d want 200", g)
	}
	if g := effectiveSubtaskBatchSize(200, syncMemHigh); g != 100 {
		t.Errorf("high: got %d want 100", g)
	}
	if g := effectiveSubtaskBatchSize(200, syncMemCritical); g != 50 {
		t.Errorf("critical: got %d want 50", g)
	}
	if g := effectiveSubtaskBatchSize(30, syncMemCritical); g != minSubtaskBatchSize {
		t.Errorf("critical floor: got %d want %d", g, minSubtaskBatchSize)
	}
}
