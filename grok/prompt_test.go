package grok

import (
	"strings"
	"testing"
)

func TestAllowedToolsEnforcesFeatureToggles(t *testing.T) {
	tools := []ToolDefinition{
		{Type: "function", Feature: "perps", Function: ToolFunction{Name: "execute_perp_trade"}},
		{Type: "function", Feature: "options", Function: ToolFunction{Name: "execute_option_trade"}},
		{Type: "function", Function: ToolFunction{Name: "get_account_summary"}},
		{Type: "function", Feature: "unknown", Function: ToolFunction{Name: "unknown_tool"}},
	}

	got := allowedTools(tools, FeaturesConfig{EnablePerps: true, EnableOptions: false})
	names := toolNames(got)
	if len(got) != 2 || names["execute_perp_trade"] == false || names["get_account_summary"] == false {
		t.Fatalf("unexpected filtered tools: %v", names)
	}
	if names["execute_option_trade"] || names["unknown_tool"] {
		t.Fatalf("disabled or unknown tool was allowed: %v", names)
	}

	if got := allowedTools(tools, FeaturesConfig{}); got != nil {
		t.Fatalf("allowedTools with all features disabled = %v, want nil", got)
	}
}

func TestFilterPromptRemovesDisabledSections(t *testing.T) {
	raw := strings.Join([]string{
		"Intro",
		"",
		"PERPS (USDC):",
		"perp details",
		"",
		"OPTIONS:",
		"option details",
		"",
		"POLYMARKET:",
		"prediction market details",
		"",
		"AUTONOMOUS MODE:",
		"autonomous details",
		"",
		"HYPE ENERGY:",
		"hype details",
		"",
		"---",
		"CHAOTIC LOSS ENERGY:",
		"chaos details",
		"",
		"---",
		"Outro",
	}, "\n")

	allDisabled := filterPrompt(raw, FeaturesConfig{})
	for _, unwanted := range []string{"PERPS (USDC)", "perp details", "OPTIONS:", "POLYMARKET:", "AUTONOMOUS MODE:", "HYPE ENERGY:", "CHAOTIC LOSS ENERGY:"} {
		if strings.Contains(allDisabled, unwanted) {
			t.Fatalf("disabled section %q was not removed:\n%s", unwanted, allDisabled)
		}
	}
	for _, wanted := range []string{"Intro", "Outro"} {
		if !strings.Contains(allDisabled, wanted) {
			t.Fatalf("unrelated text %q was removed:\n%s", wanted, allDisabled)
		}
	}

	allEnabled := filterPrompt(raw, FeaturesConfig{
		EnablePerps:       true,
		EnableOptions:     true,
		EnablePolymarket:  true,
		EnableAutonomous:  true,
		EnableHypeWins:    true,
		EnableChaoticLoss: true,
	})
	if !strings.Contains(allEnabled, "perp details") || !strings.Contains(allEnabled, "option details") {
		t.Fatalf("enabled sections were removed:\n%s", allEnabled)
	}
}

func TestApplyRiskPlaceholders(t *testing.T) {
	prompt := "risk={{DEFAULT_RISK_PER_TRADE}} heat={{MAX_PORTFOLIO_RISK}}"
	got := applyRiskPlaceholders(prompt, FeaturesConfig{
		DefaultRiskPerTrade: 1.5,
		MaxPortfolioRisk:    10,
	})
	want := "risk=1.5% heat=10%"
	if got != want {
		t.Fatalf("applyRiskPlaceholders() = %q, want %q", got, want)
	}
}

func toolNames(tools []ToolDefinition) map[string]bool {
	names := make(map[string]bool, len(tools))
	for _, tool := range tools {
		names[tool.Function.Name] = true
	}
	return names
}
