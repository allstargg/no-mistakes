package oteltrace

import (
	"context"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func emitInvocationSpans(r *Runtime, runCtx context.Context, stepContexts map[string]context.Context, invocations []db.AgentInvocation) {
	for i := range invocations {
		inv := &invocations[i]
		if inv.ID == "" || inv.StartedAt <= 0 || inv.CompletedAt <= 0 {
			continue
		}
		parent := runCtx
		if stepCtx := stepContexts[inv.StepName]; stepCtx != nil {
			parent = stepCtx
		}
		start := unixSourceTime(inv.StartedAt)
		end := unixSourceTime(inv.CompletedAt)
		if end.Before(start) {
			end = start
		}
		_, span := r.tracer.Start(
			withSpanIdentity(parent, "invocation:"+inv.ID),
			spanNameInvocation,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithTimestamp(start),
			trace.WithAttributes(invocationAttributes(inv)...),
		)
		if inv.ExitStatus == "error" || inv.ExitStatus == "cancelled" {
			span.SetStatus(codes.Error, "")
		}
		span.End(trace.WithTimestamp(end))
	}
}

func invocationAttributes(inv *db.AgentInvocation) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attrInvocationID.String(inv.ID),
		attrOperationName.String("invoke_agent"),
		attrStepName.String(registryStepName(types.StepName(inv.StepName))),
		attrOutcome.String(invocationOutcome(inv.ExitStatus)),
		attrHarnessFamily.String(normalizeHarnessFamily(inv.Agent)),
	}
	if mode, known := normalizeSessionMode(inv.SessionMode); known {
		attrs = append(attrs, attrSessionMode.String(mode))
	}
	if inv.ModelProvider != nil {
		if provider, known := normalizeKnownProvider(*inv.ModelProvider); known {
			attrs = append(attrs, attrProviderName.String(provider))
		}
	}
	if family, known := knownModelFamily(inv.Model); known {
		attrs = append(attrs, attrModelFamily.String(family))
	}
	if category := invocationFailureCategory(inv.ExitStatus, inv.FailureCategory); category != "" {
		attrs = append(attrs, attrFailureCategory.String(category))
	}
	return attrs
}

func invocationOutcome(status string) string {
	switch status {
	case "ok":
		return outcomeSuccess
	case "cancelled":
		return outcomeCancelled
	default:
		return outcomeFailed
	}
}

func normalizeSessionMode(mode string) (string, bool) {
	switch mode {
	case db.InvocationModeCold, db.InvocationModeStarted:
		return "fresh", true
	case db.InvocationModeResumed:
		return "resumed", true
	case db.InvocationModeFallback:
		return "fallback", true
	default:
		return "", false
	}
}

func invocationFailureCategory(exitStatus, category string) string {
	if exitStatus == "ok" {
		return ""
	}
	switch category {
	case "spawn":
		return failureSpawnFailed
	case "cancelled":
		return failureCancelled
	case "":
		if exitStatus == "cancelled" {
			return failureCancelled
		}
		return failureToolError
	default:
		return failureToolError
	}
}

func normalizeHarnessFamily(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "claude":
		return "claude"
	case "codex":
		return "codex"
	case "grok":
		return "grok"
	case "pi":
		return "pi"
	case "opencode":
		return "opencode"
	default:
		return "other"
	}
}

func normalizeProvider(provider string) string {
	if value, known := normalizeKnownProvider(provider); known {
		return value
	}
	return "other"
}

func normalizeKnownProvider(provider string) (string, bool) {
	value := strings.ToLower(strings.TrimSpace(provider))
	switch value {
	case "openai", "gcp.gen_ai", "gcp.vertex_ai", "gcp.gemini", "anthropic", "cohere",
		"azure.ai.inference", "azure.ai.openai", "ibm.watsonx.ai", "aws.bedrock", "perplexity",
		"xai", "deepseek", "groq", "mistral_ai":
		return value, true
	default:
		return "", false
	}
}

func normalizeModelFamily(model string) string {
	if value, known := knownModelFamily(model); known {
		return value
	}
	return "other"
}

func knownModelFamily(model string) (string, bool) {
	value := strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.HasPrefix(value, "gpt-"), strings.HasPrefix(value, "o1"), strings.HasPrefix(value, "o3"), strings.HasPrefix(value, "o4"):
		return "gpt", true
	case strings.HasPrefix(value, "claude"):
		return "claude", true
	case strings.HasPrefix(value, "gemini"):
		return "gemini", true
	case strings.HasPrefix(value, "grok"):
		return "grok", true
	case strings.HasPrefix(value, "llama"):
		return "llama", true
	default:
		return "", false
	}
}

func approvedInvocationSpanAttribute(key attribute.Key) bool {
	switch key {
	case attrInvocationID, attrOperationName, attrStepName, attrOutcome, attrSessionMode,
		attrHarnessFamily, attrProviderName, attrModelFamily, attrFailureCategory:
		return true
	default:
		return false
	}
}
