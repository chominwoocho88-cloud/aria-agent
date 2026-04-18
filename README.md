# ORCA + JACKAL

`ORCA` means `Omnidirectional Risk & Context Analyzer`.

`JACKAL` means `Just-in-time Alert for Candidates & Key Asset Leverage`.

## Repository Layout

```text
.
|-- .github/workflows/   # Scheduled jobs, backtests, reset, policy gates
|-- data/                # Runtime state and tracked JSON / SQLite data
|-- docs/                # Architecture, v2 design, release-readiness notes
|-- jackal/              # JACKAL package
|-- orca/                # ORCA package
|-- reports/             # Generated reports and dashboard output
|-- .env.example         # Example local environment variables
|-- .gitignore
|-- README.md
`-- requirements.txt
```

## Main Entry Points

```bash
python -m pip install -r requirements.txt

python -m orca
python -m orca.main
python -m orca.backtest --months 6 --walk-forward
python -m orca.reset --orca

python -m jackal
python -m jackal.core
python -m jackal.scanner
python -m jackal.backtest
python -m jackal.tracker
```

## ORCA Package Map

- `orca/main.py`: live orchestration and report assembly
- `orca/analysis.py`: verification, lessons, baseline, candidate review
- `orca/agents.py`: Hunter -> Analyst -> Devil -> Reporter
- `orca/data.py`: market-data collection and cost tracking
- `orca/notify.py`: Telegram and scheduled notifications
- `orca/state.py`: SQLite state spine, candidate registry, research history
- `orca/research_report.py`: research comparison report
- `orca/research_gate.py`: regression gate evaluation
- `orca/policy_promote.py`: promotion decision builder
- `orca/dashboard.py`: dashboard renderer
- `orca/paths.py`: canonical runtime paths

## JACKAL Package Map

- `jackal/core.py`: live opportunity-engine entrypoint
- `jackal/hunter.py`: candidate discovery pipeline
- `jackal/scanner.py`: watchlist timing evaluation (portfolio + candidate registry + recent recommendations)
- `jackal/tracker.py`: outcome tracking and weight refresh
- `jackal/evolution.py`: learning and weight evolution
- `jackal/probability.py`: candidate lesson probability adjustment
- `jackal/families.py`: canonical signal-family taxonomy
- `jackal/shield.py`: budget and secret checks
- `jackal/compact.py`: context compaction
- `jackal/market_data.py`: market-data collection
- `jackal/adapter.py`: ORCA context bridge

## Learning Loop

1. `JACKAL` finds candidates from hunt / scan / shadow flows.
2. Candidates are written into `candidate_registry` inside `orca.state`.
3. `ORCA` reviews recent candidates against the current market regime.
4. Tracker and shadow resolution write D1 / swing / follow-up outcomes.
5. Candidate lessons are generated as `aligned_win`, `opposed_loss`, and similar labels.
6. `JACKAL` reads the probability summary and applies a small score adjustment only when recent samples are sufficient.

This means the system learns from candidate quality and market alignment, not just from a fixed portfolio.

## Runtime Roles

- `ORCA Daily`: market regime report, baseline, sentiment, rotation, dashboard render
- `JACKAL Hunter`: new candidate discovery, excluding current holdings
- `JACKAL Scanner`: timing scan for current holdings and watchlist-style names
- `JACKAL Tracker`: next-day / swing outcome tracking and weight refresh

## Documents

- Candidate-registry v2 design:
  [docs/orca_candidate_registry_v2.md](/C:/Users/cho.minwoo/Desktop/aria-agent-main/docs/orca_candidate_registry_v2.md)
- Architecture and migration notes:
  [docs/orca_v2_architecture.md](/C:/Users/cho.minwoo/Desktop/aria-agent-main/docs/orca_v2_architecture.md)
- Backlog:
  [docs/orca_v2_backlog.md](/C:/Users/cho.minwoo/Desktop/aria-agent-main/docs/orca_v2_backlog.md)
- Release readiness and GitHub handoff:
  [docs/orca_jackal_release_readiness.md](/C:/Users/cho.minwoo/Desktop/aria-agent-main/docs/orca_jackal_release_readiness.md)

## Current Caveats

- `ORCA` backtest requires `ANTHROPIC_API_KEY`.
- `yfinance` rate limits can reduce backtest reliability unless cached data or retries are added.
- The current environment does not include `git`, so publishing must be done from a machine with Git installed.
