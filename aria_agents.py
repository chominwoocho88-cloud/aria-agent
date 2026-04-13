name: ARIA Daily Report

on:
  schedule:
    - cron: '30 22 * * 0-4'  # 07:30 KST - 아침 (MORNING) 월~금
    - cron: '0 13  * * 1-5'  # 22:00 KST - 저녁 EDT(여름) 후보 월~금
    - cron: '0 14  * * 1-5'  # 23:00 KST - 저녁 EST(겨울) 후보 월~금
    - cron: '0 22  * * 0'    # 일요일 07:00 KST - 주간 리포트
    - cron: '0 21  1 * *'    # 매월 1일 06:00 KST - 월간 리포트
  workflow_dispatch:

jobs:
  run-aria:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    env:
      FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

    steps:
      - name: Checkout
        uses: actions/checkout@v5

      - name: Setup Python
        uses: actions/setup-python@v5.4.0
        with:
          python-version: '3.11'

      - name: Cache pip
        id: cache-venv
        uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-anthropic-0.94.0-rich-14.3.4-httpx-0.28.1

      - name: Install dependencies
        run: pip install -r requirements.txt

      # ── 주간 리포트 (일요일) ────────────────────────────────────────────────
      - name: Run Weekly + Lessons
        if: github.event.schedule == '0 22 * * 0'
        env:
          PYTHONIOENCODING: utf-8
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          TELEGRAM_TOKEN:    ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID:  ${{ secrets.TELEGRAM_CHAT_ID }}
        run: |
          python3 << 'EOF'
          import json
          from pathlib import Path
          from aria_notify   import send_weekly_report, send_calendar_report
          from aria_analysis import extract_weekly_lessons

          send_weekly_report()
          send_calendar_report()

          memory   = json.loads(Path('memory.json').read_text(encoding='utf-8')) if Path('memory.json').exists() else []
          accuracy = json.loads(Path('accuracy.json').read_text(encoding='utf-8')) if Path('accuracy.json').exists() else {}
          extract_weekly_lessons(memory, accuracy)
          EOF

      # ── 월간 리포트 (매월 1일) ──────────────────────────────────────────────
      - name: Run Monthly + Lessons
        if: github.event.schedule == '0 21 1 * *'
        env:
          PYTHONIOENCODING: utf-8
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          TELEGRAM_TOKEN:    ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID:  ${{ secrets.TELEGRAM_CHAT_ID }}
        run: |
          python3 << 'EOF'
          import json
          from pathlib import Path
          from aria_notify   import send_monthly_report
          from aria_analysis import extract_monthly_lessons

          send_monthly_report()

          memory   = json.loads(Path('memory.json').read_text(encoding='utf-8')) if Path('memory.json').exists() else []
          accuracy = json.loads(Path('accuracy.json').read_text(encoding='utf-8')) if Path('accuracy.json').exists() else {}
          extract_monthly_lessons(memory, accuracy)
          EOF

      # ── MORNING (월~금 07:30 KST) ───────────────────────────────────────────
      - name: Run ARIA MORNING
        if: >
          github.event.schedule == '30 22 * * 0-4' ||
          github.event_name == 'workflow_dispatch'
        env:
          PYTHONIOENCODING: utf-8
          ARIA_MODE:         MORNING
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          TELEGRAM_TOKEN:    ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID:  ${{ secrets.TELEGRAM_CHAT_ID }}
          GH_TOKEN:          ${{ secrets.GH_TOKEN }}
          GH_REPO:           ${{ secrets.GH_REPO }}
          KRX_API_KEY:       ${{ secrets.KRX_API_KEY }}
          FRED_API_KEY:      ${{ secrets.FRED_API_KEY }}
          FSCAPI_KEY:        ${{ secrets.FSCAPI_KEY }}
        run: |
          python aria_main.py
          echo "ARIA_MODE=MORNING" >> $GITHUB_ENV

      # ── EVENING (미장 30분 전 — 서머타임 자동 대응) ─────────────────────────
      # 13:00 UTC(EDT여름)과 14:00 UTC(EST겨울) 둘 다 등록
      # 실행 시점에 미국 동부시간(ET) 기준으로 09:00 ±10분인지 확인 후 실행
      - name: Run ARIA EVENING
        if: >
          github.event.schedule == '0 13 * * 1-5' ||
          github.event.schedule == '0 14 * * 1-5'
        env:
          PYTHONIOENCODING: utf-8
          ARIA_MODE:         EVENING
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          TELEGRAM_TOKEN:    ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID:  ${{ secrets.TELEGRAM_CHAT_ID }}
          GH_TOKEN:          ${{ secrets.GH_TOKEN }}
          GH_REPO:           ${{ secrets.GH_REPO }}
          KRX_API_KEY:       ${{ secrets.KRX_API_KEY }}
          FRED_API_KEY:      ${{ secrets.FRED_API_KEY }}
          FSCAPI_KEY:        ${{ secrets.FSCAPI_KEY }}
        run: |
          python3 << 'EOF'
          from datetime import datetime
          from zoneinfo import ZoneInfo
          import subprocess, os, sys
          from pathlib import Path

          # ── 1. ET 시간 체크 (서머타임 자동)
          et  = ZoneInfo("America/New_York")
          now = datetime.now(et)
          h, m = now.hour, now.minute
          target_ok = (h == 8 and m >= 50) or (h == 9 and m <= 10)
          print("현재 ET: " + str(h) + ":" + str(m).zfill(2) + " " + now.strftime("%Z")
                + " — " + ("실행" if target_ok else "스킵(시간 외)"))
          if not target_ok:
              sys.exit(0)

          # ── 2. 오늘 EVENING 리포트 중복 실행 방어
          today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
          report_path = Path("reports") / (today + "_evening.json")
          if report_path.exists():
              print("⚠️ 오늘 EVENING 리포트 이미 존재 — 스킵: " + str(report_path))
              sys.exit(0)

          env = dict(os.environ)
          env["ARIA_MODE"] = "EVENING"
          subprocess.run(["python", "aria_main.py"], check=True, env=env)
          import os as _os
          with open(_os.environ.get("GITHUB_ENV", "/dev/null"), "a") as f:
              f.write("ARIA_MODE=EVENING\n")
          EOF

      # ── 저장 ────────────────────────────────────────────────────────────────
      - name: Save memory
        run: |
          git config user.name "ARIA Bot"
          git config user.email "aria@github-actions"

          backup_files() {
            mkdir -p /tmp/aria_data/data /tmp/aria_data/reports
            [ -d "data" ]    && cp -r data/.    /tmp/aria_data/data/    || true
            [ -d "reports" ] && cp -r reports/. /tmp/aria_data/reports/ || true
            [ -f "dashboard.html" ] && cp dashboard.html /tmp/aria_data/ || true
          }

          restore_files() {
            mkdir -p data reports
            ls /tmp/aria_data/data/ 2>/dev/null    && cp /tmp/aria_data/data/*    data/    || true
            ls /tmp/aria_data/reports/ 2>/dev/null && cp /tmp/aria_data/reports/* reports/ || true
            cp /tmp/aria_data/dashboard.html . 2>/dev/null || true
          }

          stage_files() {
            git add data/     2>/dev/null || true
            git add reports/  2>/dev/null || true
            git add dashboard.html 2>/dev/null || true
          }

          backup_files
          git fetch origin main
          git reset --hard origin/main
          restore_files
          stage_files
          git diff --staged --quiet || git commit -m "ARIA: $(date +'%Y-%m-%d %H:%M') [${ARIA_MODE:-AUTO}] 저장"

          git push || (
            echo "Push 실패 — 재시도"
            git fetch origin main
            git reset --hard origin/main
            restore_files
            stage_files
            git diff --staged --quiet || git commit -m "ARIA: $(date +'%Y-%m-%d %H:%M') [${ARIA_MODE:-AUTO}] 저장 (retry)"
            git push
          )
        env:
          ARIA_MODE: ${{ env.ARIA_MODE }}
