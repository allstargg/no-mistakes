// Package tracecontext owns the narrow W3C parent-context carrier accepted at
// no-mistakes process, Git hook, IPC, and persistence boundaries. It does not
// carry OpenTelemetry baggage or arbitrary metadata.
package tracecontext

import "strings"

const (
	// These process variables mirror the W3C HTTP carrier names. They are read
	// only by the short-lived invoking client, never from the daemon's ambient
	// environment.
	EnvTraceparent = "TRACEPARENT"
	EnvTracestate  = "TRACESTATE"
	EnvBaggage     = "BAGGAGE"

	// Version 00 traceparent has one exact wire size. Future W3C versions are
	// deliberately unsupported until their parsing rules are implemented.
	MaxTraceparentBytes  = 55
	MaxTracestateBytes   = 512
	MaxTracestateMembers = 32
)

// Context is the complete allowlist that may cross a no-mistakes boundary.
// Traceparent is required. Tracestate is optional. Baggage is intentionally
// absent so callers cannot turn this into a generic metadata carrier.
type Context struct {
	Traceparent string `json:"traceparent"`
	Tracestate  string `json:"tracestate,omitempty"`
}

// Diagnostic is a bounded, value-free reason why incoming carrier data was
// ignored. It is safe to print or log because it never includes the input.
type Diagnostic string

const (
	DiagnosticTraceparentInvalid      Diagnostic = "traceparent is malformed"
	DiagnosticTraceparentOversized    Diagnostic = "traceparent exceeds the supported size"
	DiagnosticTraceparentUnsupported  Diagnostic = "traceparent version is unsupported"
	DiagnosticTracestateWithoutParent Diagnostic = "tracestate has no valid traceparent"
	DiagnosticTracestateInvalid       Diagnostic = "tracestate is malformed and was omitted"
	DiagnosticTracestateOversized     Diagnostic = "tracestate exceeds the supported size and was omitted"
	DiagnosticTraceparentDuplicate    Diagnostic = "duplicate traceparent carrier was ignored"
	DiagnosticTracestateDuplicate     Diagnostic = "duplicate tracestate carrier was ignored"
	DiagnosticBaggageUnsupported      Diagnostic = "baggage is not accepted"
)

func (d Diagnostic) String() string { return string(d) }

// Result contains an accepted context, if any, and bounded diagnostics for
// rejected fields. A malformed optional tracestate does not discard an
// otherwise valid parent.
type Result struct {
	Context     *Context
	Diagnostics []Diagnostic
}

// FromEnvironment captures the explicit process allowlist from an invoking
// client. getenv is injected to keep validation deterministic in tests.
func FromEnvironment(getenv func(string) string) Result {
	if getenv == nil {
		return Result{}
	}
	result := Parse(getenv(EnvTraceparent), getenv(EnvTracestate))
	if getenv(EnvBaggage) != "" {
		result.Diagnostics = append(result.Diagnostics, DiagnosticBaggageUnsupported)
	}
	return result
}

// Parse validates incoming W3C carrier fields. It supports traceparent version
// 00 and the W3C tracestate grammar, with protocol limits applied before any
// value crosses IPC or persistence boundaries.
func Parse(traceparent, tracestate string) Result {
	if traceparent == "" {
		if tracestate == "" {
			return Result{}
		}
		return Result{Diagnostics: []Diagnostic{DiagnosticTracestateWithoutParent}}
	}
	if len(traceparent) > MaxTraceparentBytes {
		if len(traceparent) >= 2 && traceparent[:2] != "00" {
			return Result{Diagnostics: []Diagnostic{DiagnosticTraceparentUnsupported}}
		}
		return Result{Diagnostics: []Diagnostic{DiagnosticTraceparentOversized}}
	}
	if len(traceparent) >= 2 && traceparent[:2] != "00" {
		return Result{Diagnostics: []Diagnostic{DiagnosticTraceparentUnsupported}}
	}
	if !validTraceparentV00(traceparent) {
		return Result{Diagnostics: []Diagnostic{DiagnosticTraceparentInvalid}}
	}

	ctx := &Context{Traceparent: traceparent}
	if tracestate == "" {
		return Result{Context: ctx}
	}
	if len(tracestate) > MaxTracestateBytes {
		return Result{Context: ctx, Diagnostics: []Diagnostic{DiagnosticTracestateOversized}}
	}
	if !validTracestate(tracestate) {
		return Result{Context: ctx, Diagnostics: []Diagnostic{DiagnosticTracestateInvalid}}
	}
	ctx.Tracestate = tracestate
	return Result{Context: ctx}
}

// Validate revalidates a typed boundary value. Daemon ingress uses it even
// when the invoking client already validated the same context.
func Validate(ctx *Context) Result {
	if ctx == nil {
		return Result{}
	}
	return Parse(ctx.Traceparent, ctx.Tracestate)
}

func validTraceparentV00(value string) bool {
	if len(value) != MaxTraceparentBytes || value[2] != '-' || value[35] != '-' || value[52] != '-' {
		return false
	}
	if value[:2] != "00" || !lowerHex(value[3:35]) || !lowerHex(value[36:52]) || !lowerHex(value[53:55]) {
		return false
	}
	return !allZero(value[3:35]) && !allZero(value[36:52])
}

func lowerHex(value string) bool {
	for i := 0; i < len(value); i++ {
		if (value[i] < '0' || value[i] > '9') && (value[i] < 'a' || value[i] > 'f') {
			return false
		}
	}
	return true
}

func allZero(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] != '0' {
			return false
		}
	}
	return true
}

func validTracestate(value string) bool {
	// W3C allows optional SP/HTAB around commas and the member equals sign,
	// but not before the first member or after the last value.
	if strings.Trim(value, " \t") != value {
		return false
	}
	members := strings.Split(value, ",")
	if len(members) == 0 || len(members) > MaxTracestateMembers {
		return false
	}
	seen := make(map[string]struct{}, len(members))
	for _, rawMember := range members {
		member := strings.Trim(rawMember, " \t")
		rawKey, rawValue, ok := strings.Cut(member, "=")
		key := strings.TrimRight(rawKey, " \t")
		val := strings.TrimLeft(rawValue, " \t")
		if !ok || !validTracestateKey(key) || !validTracestateValue(val) {
			return false
		}
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}

func validTracestateKey(key string) bool {
	if key == "" || len(key) > 256 {
		return false
	}
	if at := strings.IndexByte(key, '@'); at >= 0 {
		if strings.LastIndexByte(key, '@') != at || at < 1 || at > 241 || len(key)-at-1 < 1 || len(key)-at-1 > 14 {
			return false
		}
		if !lowerAlphaOrDigit(key[0]) || !lowerAlpha(key[at+1]) {
			return false
		}
		return validKeyTail(key[1:at]) && validKeyTail(key[at+2:])
	}
	return lowerAlpha(key[0]) && validKeyTail(key[1:])
}

func validKeyTail(value string) bool {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if !lowerAlphaOrDigit(c) && c != '_' && c != '-' && c != '*' && c != '/' {
			return false
		}
	}
	return true
}

func lowerAlpha(c byte) bool { return c >= 'a' && c <= 'z' }

func lowerAlphaOrDigit(c byte) bool {
	return lowerAlpha(c) || (c >= '0' && c <= '9')
}

func validTracestateValue(value string) bool {
	if value == "" || len(value) > 256 || value[len(value)-1] == ' ' {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 || c > 0x7e || c == ',' || c == '=' {
			return false
		}
	}
	return true
}
