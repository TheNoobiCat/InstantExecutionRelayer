package grok

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// LoadAndPreparePrompt loads the system prompt from the configured path (if any),
// applies feature-based filtering, and returns the final prompt string.
func LoadAndPreparePrompt(cfg *Config) string {
	raw := cfg.Chat.SystemPrompt

	if path := strings.TrimSpace(cfg.Chat.SystemPromptPath); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			logrus.WithError(err).Warnf("failed to read system prompt file %s, using inline prompt", path)
		} else {
			raw = string(data)
		}
	}

	filtered := filterPrompt(raw, cfg.Features)
	return applyRiskPlaceholders(filtered, cfg.Features)
}

// filterPrompt removes sections for disabled features to save tokens.
func filterPrompt(raw string, feat FeaturesConfig) string {
	lines := strings.Split(raw, "\n")
	out := make([]string, 0, len(lines))

	skipUntilBlank := false
	skipUntilSeparator := false

	shouldSkip := func(trimmed string) bool {
		upper := strings.ToUpper(trimmed)
		switch {
		case !feat.EnablePerps && strings.HasPrefix(upper, "PERPS ("):
			return true
		case !feat.EnableOptions && (strings.HasPrefix(upper, "OPTIONS:") || strings.HasPrefix(upper, "**OPTIONS-SPECIFIC TOOLS:**")):
			return true
		case !feat.EnablePolymarket && strings.HasPrefix(upper, "POLYMARKET:"):
			return true
		case !feat.EnableAutonomous && strings.HasPrefix(upper, "AUTONOMOUS MODE:"):
			return true
		case !feat.EnableAutonomous && strings.HasPrefix(upper, "[AUTONOMOUS MODE"):
			return true
		case !feat.EnableHypeWins && strings.HasPrefix(upper, "HYPE ENERGY:"):
			return true
		case !feat.EnableChaoticLoss && strings.HasPrefix(upper, "CHAOTIC LOSS ENERGY:"):
			return true
		default:
			return false
		}
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if shouldSkip(trimmed) {
			// Multi-line sections detection
			if strings.HasPrefix(strings.ToUpper(trimmed), "[AUTONOMOUS MODE") ||
				strings.HasPrefix(strings.ToUpper(trimmed), "HYPE ENERGY:") ||
				strings.HasPrefix(strings.ToUpper(trimmed), "CHAOTIC LOSS ENERGY:") {
				skipUntilSeparator = true
			} else {
				skipUntilBlank = true
			}
			continue
		}

		if skipUntilSeparator {
			// End multi-line skip on separator or new section header
			if strings.HasPrefix(trimmed, "---") ||
				(strings.HasPrefix(trimmed, "[") && strings.Contains(trimmed, "]")) {
				skipUntilSeparator = false
				// Don't skip the separator/section line itself
			} else {
				continue
			}
		}

		if skipUntilBlank {
			if trimmed == "" {
				skipUntilBlank = false
			}
			continue
		}

		out = append(out, line)
	}

	return strings.TrimSpace(strings.Join(out, "\n"))
}

// applyRiskPlaceholders injects risk numbers into the prompt.
func applyRiskPlaceholders(prompt string, feat FeaturesConfig) string {
	repl := map[string]string{
		"{{DEFAULT_RISK_PER_TRADE}}": fmt.Sprintf("%.4g%%", feat.DefaultRiskPerTrade),
		"{{MAX_PORTFOLIO_RISK}}":     fmt.Sprintf("%.4g%%", feat.MaxPortfolioRisk),
	}
	out := prompt
	for k, v := range repl {
		out = strings.ReplaceAll(out, k, v)
	}
	return out
}
