package metrics

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
)

// TestCounter_NilSafe pins the zero-value contract the whole package
// relies on: every Record* method (including the new ADR-0064 ones)
// must be a cheap no-op when the *Counter receiver is nil. Tests,
// legacy paths, and partial wiring scenarios pass nil — any panic
// here would surface as a production crash on code paths that
// forgot to plumb metrics through.
func TestCounter_NilSafe(t *testing.T) {
	var c *Counter // deliberately nil
	// Should not panic or allocate.
	c.RecordPendingSize(0, 10)
	c.RecordCheckpointPublish(0, true)
	c.RecordPublishRetry("Publish", 2)
	c.RecordCatchUpApplied(0, 100)
	// ADR-0064 M2d additions.
	c.RecordStartupDuration(0, StartupModeLabelOnDemand, 1.2, true)
	c.RecordOnDemandRPCDuration(0, "ok", 0.2)
	c.RecordSentinelProduceDuration(0, true, 0.05)
}

// TestNewCounter_RegistersADR0064Series verifies every new histogram
// is registered on the passed Registerer. A drift between
// NewCounter's field initialisation and its MustRegister calls
// would silently drop metrics at runtime — this pins the invariant.
func TestNewCounter_RegistersADR0064Series(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewCounter(reg)
	if c.StartupDurationSec == nil {
		t.Error("StartupDurationSec not constructed")
	}
	if c.OnDemandRPCDurationSec == nil {
		t.Error("OnDemandRPCDurationSec not constructed")
	}
	if c.SentinelProduceDurationSec == nil {
		t.Error("SentinelProduceDurationSec not constructed")
	}

	// Drive one observation into each so the series shows up in
	// the registry.
	c.RecordStartupDuration(3, StartupModeLabelOnDemand, 1.2, true)
	c.RecordOnDemandRPCDuration(3, "ok", 0.5)
	c.RecordSentinelProduceDuration(3, true, 0.05)

	got, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := map[string]bool{
		"counter_startup_duration_seconds":          false,
		"counter_ondemand_rpc_duration_seconds":     false,
		"counter_sentinel_produce_duration_seconds": false,
	}
	for _, mf := range got {
		if _, tracked := want[mf.GetName()]; tracked {
			want[mf.GetName()] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("metric %q missing from registry", name)
		}
	}
}

// TestNewCounter_DoubleRegisterPanics locks the MustRegister path:
// constructing two Counters on the same Registerer should panic.
// Dashboards and alerts key off a single metric family; duplicate
// registration would mean an operator typo in main.go silently
// shadows real metrics instead of failing loudly.
func TestNewCounter_DoubleRegisterPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewCounter(reg)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate NewCounter against same Registerer")
		}
	}()
	_ = NewCounter(reg)
}

// TestRecordStartupDuration_LabelsMatchExpectations feeds a small
// matrix through RecordStartupDuration and inspects the emitted
// series for the mode/result label pairs we rely on in the ADR
// dashboards + alerts.
func TestRecordStartupDuration_LabelsMatchExpectations(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewCounter(reg)

	// Emit: on-demand success, on-demand error, fallback success.
	c.RecordStartupDuration(7, StartupModeLabelOnDemand, 1.2, true)
	c.RecordStartupDuration(7, StartupModeLabelOnDemand, 3.1, false)
	c.RecordStartupDuration(7, StartupModeLabelFallback, 11.0, true)

	got, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	type labelKey struct{ vshard, mode, result string }
	seen := map[labelKey]uint64{}
	for _, mf := range got {
		if mf.GetName() != "counter_startup_duration_seconds" {
			continue
		}
		for _, m := range mf.Metric {
			k := labelKey{}
			for _, l := range m.Label {
				switch l.GetName() {
				case "vshard":
					k.vshard = l.GetValue()
				case "mode":
					k.mode = l.GetValue()
				case "result":
					k.result = l.GetValue()
				}
			}
			seen[k] = m.GetHistogram().GetSampleCount()
		}
	}
	want := []struct {
		k     labelKey
		count uint64
	}{
		{labelKey{"7", "on-demand", "success"}, 1},
		{labelKey{"7", "on-demand", "error"}, 1},
		{labelKey{"7", "fallback", "success"}, 1},
	}
	for _, tc := range want {
		if got := seen[tc.k]; got != tc.count {
			t.Errorf("label %+v count = %d, want %d (all seen: %+v)", tc.k, got, tc.count, seen)
		}
	}
}

// TestOnDemandRPCResultLabel exhaustively maps the ADR-0064 §4
// fallback-class codes (plus raw ctx errors) to their stable metric
// labels. Any drift here would disconnect logs from dashboard
// filters.
func TestOnDemandRPCResultLabel(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "ok"},
		{"ctx deadline raw", context.DeadlineExceeded, "timeout"},
		{"ctx canceled raw", context.Canceled, "canceled"},
		{"deadline exceeded status", connect.NewError(connect.CodeDeadlineExceeded, errors.New("x")), "timeout"},
		{"canceled status", connect.NewError(connect.CodeCanceled, errors.New("x")), "canceled"},
		{"unavailable", connect.NewError(connect.CodeUnavailable, errors.New("x")), "unavailable"},
		{"failed_precondition", connect.NewError(connect.CodeFailedPrecondition, errors.New("x")), "failed_precondition"},
		{"resource_exhausted", connect.NewError(connect.CodeResourceExhausted, errors.New("x")), "resource_exhausted"},
		{"unimplemented", connect.NewError(connect.CodeUnimplemented, errors.New("x")), "unimplemented"},
		{"internal", connect.NewError(connect.CodeInternal, errors.New("x")), "internal"},
		{"invalid argument falls to other", connect.NewError(connect.CodeInvalidArgument, errors.New("x")), "other"},
		{"unknown falls to other", errors.New("plain error"), "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := OnDemandRPCResultLabel(tc.err); got != tc.want {
				t.Errorf("OnDemandRPCResultLabel(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestStartupModeLabels_Stable is a constants sanity check. The
// labels are duplicated between this package (for metric emission)
// and counter/internal/worker (the StartupMode enum's String
// output). If operators grep dashboards for "on-demand" and the
// worker starts emitting "OnDemand" or "ondemand", alerts break
// silently.
func TestStartupModeLabels_Stable(t *testing.T) {
	if StartupModeLabelOnDemand != "on-demand" {
		t.Errorf("StartupModeLabelOnDemand = %q, want %q", StartupModeLabelOnDemand, "on-demand")
	}
	if StartupModeLabelFallback != "fallback" {
		t.Errorf("StartupModeLabelFallback = %q, want %q", StartupModeLabelFallback, "fallback")
	}
}
