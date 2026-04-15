name: ARIA Backtest

on:
  workflow_dispatch:

jobs:
  run-backtest:
    runs-on: ubuntu-latest
    env:
      FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true  # Node.js 24 opt-in (deprecation 경고 해결)
    permissions:
      contents: write

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: pip

      - name: Install dependencies
        run: pip install anthropic rich httpx yfinance pandas

      - name: Run ARIA Backtest
        env:
          PYTHONIOENCODING: utf-8
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
        run: |
          python aria_backtest.py

      - name: Run Jackal Backtest
        env:
          PYTHONIOENCODING: utf-8
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
        run: |
          python jackal/jackal_backtest.py

      - name: Save learning data
        run: |
          git config user.name  "ARIA Bot"
          git config user.email "aria@github-actions"

          git add data/accuracy.json     2>/dev/null || true
          git add data/aria_lessons.json 2>/dev/null || true
          git add data/aria_weights.json 2>/dev/null || true
          git add jackal/jackal_weights.json 2>/dev/null || true

          git stash
          git fetch origin main
          git reset --hard origin/main
          git stash pop || true

          git add data/accuracy.json     2>/dev/null || true
          git add data/aria_lessons.json 2>/dev/null || true
          git add data/aria_weights.json 2>/dev/null || true
          git add jackal/jackal_weights.json 2>/dev/null || true
          git add dashboard.html         2>/dev/null || true   # 백테스트 후 대시보드 반영
          git add data/memory.json       2>/dev/null || true   # 메모리 누적 상태 저장
          git add jackal/jackal_shadow_log.json 2>/dev/null || true  # shadow outcome 추적

          git diff --staged --quiet || \
            git commit -m "🎓 Backtest: ARIA + Jackal 학습 완료 ($(date +'%Y-%m-%d'))"
          git push || (git pull --rebase origin main && git push)
