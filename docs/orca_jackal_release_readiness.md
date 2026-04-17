# ORCA + JACKAL Release Readiness

## 1. Current Architecture

### ORCA

- `orca.main`: collects market data, runs the ORCA multi-agent analysis, stores reports, and reviews recent JACKAL candidates.
- `orca.analysis`: builds baseline context, verification, lessons, and candidate-review logic.
- `orca.state`: central SQLite spine for runs, predictions, backtests, candidate registry, candidate reviews, outcomes, and lessons.
- `orca.research_report` / `orca.research_gate` / `orca.policy_promote`: research comparison, regression gating, and promotion decision flow.

### JACKAL

- `jackal.hunter`: discovers candidate entries.
- `jackal.scanner`: scores candidates with Analyst -> Devil -> Final logic.
- `jackal.tracker`: resolves outcomes and refreshes weights.
- `jackal.evolution`: updates learning state from completed outcomes.
- `jackal.probability`: reads candidate-lesson summaries and applies small score adjustments.
- `jackal.families`: canonical signal-family taxonomy shared across Hunter, Scanner, and ORCA.

## 2. Learning Flow Check

The current learning loop is now structurally natural:

1. JACKAL hunt / scan / shadow writes candidates into `candidate_registry`.
2. ORCA reviews recent candidates against the current market regime.
3. Tracker and shadow resolution write candidate outcomes.
4. Candidate lessons are generated from alignment plus outcome:
   - `aligned_win`
   - `aligned_loss`
   - `neutral_win`
   - `neutral_loss`
   - `opposed_win`
   - `opposed_loss`
5. JACKAL reads those lessons through `summarize_candidate_probabilities()` and only applies small score changes when sample counts are sufficient.

This means the system is no longer learning only from a static portfolio or one-off logs. It is learning from:

- candidate quality
- signal family
- ORCA alignment
- realized result

## 3. Accuracy Expectation

Current accuracy should be treated as an operating estimate, not a guaranteed metric:

- ORCA directional quality:
  around `54% to 62%` in stable regimes
- JACKAL swing candidate quality:
  around `58% to 66%` when provider quality is healthy
- JACKAL D1 quality:
  around `50% to 57%`
- Counter-trend candidate strength:
  useful, but usually lower sample size and more volatile

These ranges can degrade sharply when:

- `yfinance` rate limits block market data
- ORCA backtest cannot call Anthropic due missing API keys
- research sample size is too low

## 4. What Is Working Well

- Runtime state and research state are separated.
- Candidate-registry v2 is live and integrated into the learning loop.
- Hunter and Scanner now share the same canonical family taxonomy.
- ORCA reports now expose trusted families, cautious families, aligned strengths, and counter-trend strengths.
- Probability-based score nudges are intentionally small and sample-gated, which reduces overfitting risk.

## 5. Remaining Risks Before Public GitHub Use

### Operational

- This environment does not currently provide `git`, so push / branch / PR actions must be done from a machine with Git installed.
- `ORCA` backtest still depends on `ANTHROPIC_API_KEY`.
- `yfinance` rate limits are still the biggest external reliability risk.

### Structural

- Legacy JACKAL JSON compatibility files still exist for some runtime paths.
- Offline / cached market-data mode is not complete yet.
- Research quality is still heavily constrained by live data-provider availability.

## 6. Recommended Publish Checklist

Before pushing to GitHub:

1. Copy `.env.example` and document which secrets are required in the GitHub repo settings.
2. Verify that no live secrets exist in tracked files.
3. Run:
   - `python -m py_compile orca/*.py jackal/*.py`
   - `python -m orca.research_report`
   - `python -m orca.research_gate`
4. If API keys are available, run:
   - `python -m orca.backtest --months 6 --walk-forward`
   - `python -m jackal.backtest`
5. Confirm that generated runtime artifacts are ignored by `.gitignore`.
6. Push from a machine that has Git installed.

## 7. Best Next Technical Steps

1. Add a cached historical market-data layer so backtests do not fail on provider throttling.
2. Surface family-by-alignment performance in research comparison artifacts as well, not only live ORCA reports.
3. Gradually remove remaining JACKAL legacy JSON mirrors after enough runtime soak time.
