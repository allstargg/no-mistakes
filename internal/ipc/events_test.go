package ipc

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestEventCursorRoundTrip(t *testing.T) {
	for _, seq := range []int64{0, 1, 42, 9_007_199_254_740_993} {
		encoded := EncodeEventCursor(seq)
		if !strings.HasPrefix(encoded, "nm1:") {
			t.Fatalf("cursor %q missing version prefix", encoded)
		}
		decoded, err := DecodeEventCursor(encoded)
		if err != nil {
			t.Fatalf("decode %q: %v", encoded, err)
		}
		if decoded != seq {
			t.Fatalf("round trip = %d, want %d", decoded, seq)
		}
	}
}

func TestDecodeEventCursorRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "0", "42", "nm1:", "nm1:-1", "nm1:abc", "nm2:1", "nm1:1.5", "  nm1:1", "nm1:99999999999999999999999999"} {
		if _, err := DecodeEventCursor(bad); !errors.Is(err, ErrEventCursorMalformed) {
			t.Fatalf("decode %q err = %v, want ErrEventCursorMalformed", bad, err)
		}
	}
}

func TestCompileEventFilterMatchAll(t *testing.T) {
	compiled, err := CompileEventFilter(nil)
	if err != nil {
		t.Fatalf("nil filter: %v", err)
	}
	run := "run-1"
	if !compiled.Matches(&run, "io.no_mistakes.run.created.v1") {
		t.Fatal("nil filter should match everything")
	}
	if !compiled.Matches(nil, "io.no_mistakes.run.created.v1") {
		t.Fatal("nil filter should match a run-less event")
	}
}

func TestCompileEventFilterSelectors(t *testing.T) {
	compiled, err := CompileEventFilter(&EventFilter{
		RunIDs: []string{"run-a", "run-b"},
		Types:  []string{"io.no_mistakes.step.started.v1"},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	runA, runC := "run-a", "run-c"
	if !compiled.Matches(&runA, "io.no_mistakes.step.started.v1") {
		t.Fatal("run-a + matching type should match")
	}
	if compiled.Matches(&runA, "io.no_mistakes.run.created.v1") {
		t.Fatal("non-matching type must be excluded")
	}
	if compiled.Matches(&runC, "io.no_mistakes.step.started.v1") {
		t.Fatal("non-matching run must be excluded")
	}
	if compiled.Matches(nil, "io.no_mistakes.step.started.v1") {
		t.Fatal("run-less event must be excluded by a run-ID filter")
	}
}

func TestCompileEventFilterRejectsInvalid(t *testing.T) {
	tooManyRuns := make([]string, MaxEventFilterRunIDs+1)
	for i := range tooManyRuns {
		tooManyRuns[i] = "run"
	}
	tooManyTypes := make([]string, MaxEventFilterTypes+1)
	for i := range tooManyTypes {
		tooManyTypes[i] = "io.no_mistakes.run.created.v1"
	}
	cases := map[string]*EventFilter{
		"too many run ids": {RunIDs: tooManyRuns},
		"too many types":   {Types: tooManyTypes},
		"empty run id":     {RunIDs: []string{""}},
		"whitespace runid": {RunIDs: []string{"run 1"}},
		"newline runid":    {RunIDs: []string{"run\n1"}},
		"empty type":       {Types: []string{""}},
		"space type":       {Types: []string{"io.no_mistakes run"}},
		"slash type":       {Types: []string{"io/no_mistakes"}},
	}
	for name, filter := range cases {
		if _, err := CompileEventFilter(filter); !errors.Is(err, ErrEventFilterInvalid) {
			t.Fatalf("%s: err = %v, want ErrEventFilterInvalid", name, err)
		}
	}
}

func TestCapabilitiesResultSupports(t *testing.T) {
	result := &CapabilitiesResult{Capabilities: []Capability{
		{Name: CapabilitySubscribeEvents, Versions: []int{1}},
	}}
	if !result.Supports(CapabilitySubscribeEvents, SubscribeEventsVersion) {
		t.Fatal("advertised capability/version should be supported")
	}
	if result.Supports(CapabilitySubscribeEvents, 2) {
		t.Fatal("unadvertised version must not be reported supported")
	}
	if result.Supports("other", 1) {
		t.Fatal("unadvertised capability must not be reported supported")
	}
	var nilResult *CapabilitiesResult
	if nilResult.Supports(CapabilitySubscribeEvents, 1) {
		t.Fatal("nil result supports nothing")
	}
}

func TestEventErrorFromRPCMapsCodes(t *testing.T) {
	cases := map[int]error{
		ErrMethodNotFound:         ErrEventCapabilityUnsupported,
		ErrCodeUnsupportedVersion: ErrEventVersionUnsupported,
		ErrCodeInvalidFilter:      ErrEventFilterInvalid,
		ErrCodeInvalidCursor:      ErrEventCursorMalformed,
		ErrCodeCursorExpired:      ErrEventCursorExpired,
	}
	for code, want := range cases {
		got := eventErrorFromRPC(&RPCError{Code: code, Message: "x"})
		if !errors.Is(got, want) {
			t.Fatalf("code %d mapped to %v, want %v", code, got, want)
		}
	}
	// An unmapped code is surfaced verbatim.
	passthrough := &RPCError{Code: ErrInternal, Message: "boom"}
	if got := eventErrorFromRPC(passthrough); got != passthrough {
		t.Fatalf("unmapped code = %v, want passthrough RPCError", got)
	}
}

func TestCodedErrorReportsCode(t *testing.T) {
	err := codedError(ErrCodeCursorExpired, ErrEventCursorExpired)
	var coder interface{ RPCCode() int }
	if !errors.As(error(err), &coder) {
		t.Fatal("CodedError should expose RPCCode")
	}
	if coder.RPCCode() != ErrCodeCursorExpired {
		t.Fatalf("code = %d, want %d", coder.RPCCode(), ErrCodeCursorExpired)
	}
	if err.Error() != ErrEventCursorExpired.Error() {
		t.Fatalf("message = %q, want %q", err.Error(), ErrEventCursorExpired.Error())
	}
}

// MetadataEventInfo must expose only classification and correlation metadata.
// A payload/content field would breach the prototype's content-exclusion
// guarantee, so guard the serialized shape.
func TestMetadataEventInfoHasNoContentField(t *testing.T) {
	run := "run-1"
	info := MetadataEventInfo{
		Sequence:        7,
		EventID:         "evt",
		Type:            "io.no_mistakes.run.created.v1",
		PayloadSchema:   "io.no_mistakes.run.created.v1",
		PayloadVersion:  1,
		ContentClass:    "metadata",
		SourceTimestamp: 1000,
		RecordedAt:      2000,
		RunID:           &run,
	}
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"content", "payload", "payload_json", "data", "body", "args", "arguments", "output"} {
		if _, present := generic[forbidden]; present {
			t.Fatalf("MetadataEventInfo exposed forbidden field %q", forbidden)
		}
	}
	if generic["content_class"] == nil {
		t.Fatal("content_class classification must be present")
	}
}

func TestSubscribeEventsParamsRoundTrip(t *testing.T) {
	params := SubscribeEventsParams{
		Version: SubscribeEventsVersion,
		Cursor:  EncodeEventCursor(12),
		Filter:  &EventFilter{RunIDs: []string{"run-a"}, Types: []string{"io.no_mistakes.step.started.v1"}},
	}
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	var back SubscribeEventsParams
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Version != params.Version || back.Cursor != params.Cursor {
		t.Fatalf("round trip = %#v, want %#v", back, params)
	}
	if back.Filter == nil || len(back.Filter.RunIDs) != 1 || back.Filter.RunIDs[0] != "run-a" {
		t.Fatalf("filter round trip = %#v", back.Filter)
	}
}
