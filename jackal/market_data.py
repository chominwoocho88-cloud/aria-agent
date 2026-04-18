"""
JACKAL market data helpers.

This module keeps the JACKAL data contract small and explicit:
- macro data from FRED
- optional local/public Korean market data
- local ORCA sentiment snapshot
- per-ticker technicals from yfinance
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import yfinance as yf

from orca.paths import SENTIMENT_FILE, atomic_write_json

log = logging.getLogger("jackal_market_data")

KST = timezone(timedelta(hours=9))
_BASE = Path(__file__).resolve().parent
_DATA_DIR = _BASE.parent / "data"
TECHNICAL_CACHE_FILE = _DATA_DIR / "jackal_technicals_cache.json"
TECHNICAL_CACHE_TTL_HOURS = 12
TECHNICAL_STALE_FALLBACK_HOURS = 72


def _latest_business_day() -> str:
    day = datetime.now(KST) - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day.strftime("%Y%m%d")


def _to_float(value) -> float | None:
    if value in (None, "", ".", "-"):
        return None
    try:
        return round(float(str(value).replace(",", "")), 2)
    except Exception:
        return None


def fetch_fred() -> dict:
    """Fetch a small FRED macro snapshot used by JACKAL prompts."""
    result = {
        "vix": None,
        "hy_spread": None,
        "yield_curve": None,
        "consumer_sent": None,
        "dxy": None,
        "source": False,
    }
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        log.warning("FRED_API_KEY missing")
        return result

    series = {
        "VIXCLS": "vix",
        "BAMLH0A0HYM2": "hy_spread",
        "T10Y2Y": "yield_curve",
        "UMCSENT": "consumer_sent",
        "DTWEXBGS": "dxy",
    }
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    success = 0

    for series_id, key in series.items():
        try:
            response = httpx.get(
                base_url,
                params={
                    "series_id": series_id,
                    "api_key": api_key,
                    "sort_order": "desc",
                    "limit": 5,
                    "file_type": "json",
                },
                timeout=8,
            )
            if response.status_code != 200:
                continue
            observations = [
                item
                for item in response.json().get("observations", [])
                if item.get("value") not in ("", ".")
            ]
            if not observations:
                continue
            value = _to_float(observations[0].get("value"))
            if value is None:
                continue
            result[key] = value
            success += 1
        except Exception as exc:
            log.warning("FRED %s fetch failed: %s", series_id, exc)

    result["source"] = success >= 2
    return result


def fetch_krx() -> dict:
    """
    Fetch a light KRX snapshot.

    The scanner currently does not depend on this payload, so failures should
    degrade quietly instead of blocking the JACKAL pipeline.
    """
    result = {
        "kospi": None,
        "kosdaq": None,
        "source": False,
    }
    api_key = os.environ.get("KRX_API_KEY", "").strip()
    if not api_key:
        log.info("KRX_API_KEY missing; skipping KRX snapshot")
        return result

    base_url = "https://data-dbg.krx.co.kr/svc/apis/idx/krx_dd_trd"
    success = 0
    for code, key in (("1001", "kospi"), ("2001", "kosdaq")):
        try:
            response = httpx.get(
                base_url,
                params={"basDd": _latest_business_day(), "idxIndCd": code},
                headers={"AUTH_KEY": api_key},
                timeout=8,
            )
            if response.status_code != 200:
                continue
            payload = response.json()
            data = payload.get("OutBlock_1") or payload.get("output") or {}
            value = _to_float(
                data.get("clpr")
                or data.get("close")
                or data.get("idx_clpr")
                or data.get("IDX_CLPR")
            )
            if value is None:
                continue
            result[key] = value
            success += 1
        except Exception as exc:
            log.warning("KRX %s fetch failed: %s", key, exc)

    result["source"] = success >= 1
    return result


def fetch_fsc() -> dict:
    """Fetch a few public Korean reference prices from the data.go.kr feed."""
    result = {
        "samsung": None,
        "sk_hynix": None,
        "gold": None,
        "oil_diesel": None,
        "source": False,
    }
    api_key = os.environ.get("FSCAPI_KEY", "").strip()
    if not api_key:
        log.info("FSCAPI_KEY missing; skipping FSC snapshot")
        return result

    base_url = "https://apis.data.go.kr/1160100/service"
    date_str = _latest_business_day()

    def _get(endpoint: str, params: dict) -> list[dict]:
        try:
            response = httpx.get(
                f"{base_url}{endpoint}",
                params={
                    "serviceKey": api_key,
                    "numOfRows": "10",
                    "pageNo": "1",
                    "resultType": "json",
                    **params,
                },
                timeout=8,
            )
            if response.status_code != 200:
                return []
            body = response.json().get("response", {}).get("body", {})
            items = body.get("items", {})
            item = items.get("item", []) if isinstance(items, dict) else []
            if isinstance(item, list):
                return item
            return [item] if item else []
        except Exception as exc:
            log.warning("FSC endpoint %s failed: %s", endpoint, exc)
            return []

    success = 0

    for code, key in (("005930", "samsung"), ("000660", "sk_hynix")):
        rows = _get(
            "/GetStockSecuritiesInfoService/getStockPriceInfo",
            {"likeSrtnCd": code, "basDd": date_str},
        )
        if rows:
            value = _to_float(rows[0].get("clpr"))
            if value is not None:
                result[key] = value
                success += 1

    gold_rows = _get(
        "/GetGeneralProductInfoService/getGoldPriceInfo",
        {"basDd": date_str},
    )
    for row in gold_rows:
        name = str(row.get("itmsNm", ""))
        if "99.99" in name and "1kg" in name:
            value = _to_float(row.get("clpr"))
            if value is not None:
                result["gold"] = value
                success += 1
            break
    if result["gold"] is None and gold_rows:
        value = _to_float(gold_rows[0].get("clpr"))
        if value is not None:
            result["gold"] = value
            success += 1

    oil_rows = _get(
        "/GetGeneralProductInfoService/getOilPriceInfo",
        {"basDd": date_str},
    )
    for row in oil_rows:
        category = str(row.get("oilCtg", ""))
        if "경유" not in category:
            continue
        value = _to_float(row.get("wtAvgPrcCptn") or row.get("clpr"))
        if value is None:
            continue
        result["oil_diesel"] = value
        success += 1
        break

    result["source"] = success >= 2
    return result


def load_sentiment() -> dict:
    """Load the latest ORCA sentiment snapshot if it exists."""
    if not SENTIMENT_FILE.exists():
        return {"score": 50, "level": "중립", "trend": "정보없음", "regime": ""}
    try:
        data = json.loads(SENTIMENT_FILE.read_text(encoding="utf-8"))
        current = data.get("current", {})
        return {
            "score": current.get("score", 50),
            "level": current.get("level", "중립"),
            "trend": current.get("trend", "정보없음"),
            "regime": current.get("regime", ""),
        }
    except Exception:
        return {"score": 50, "level": "중립", "trend": "정보없음", "regime": ""}


def _load_technical_cache() -> dict:
    if not TECHNICAL_CACHE_FILE.exists():
        return {}
    try:
        return json.loads(TECHNICAL_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_technical_cache(cache: dict) -> None:
    try:
        atomic_write_json(TECHNICAL_CACHE_FILE, cache)
    except Exception as exc:
        log.debug("technical cache save skipped: %s", exc)


def _load_cached_technicals(ticker: str, *, max_age_hours: int) -> dict | None:
    cache = _load_technical_cache()
    entry = cache.get(ticker)
    if not isinstance(entry, dict):
        return None
    fetched_at = str(entry.get("fetched_at", "")).strip()
    technicals = entry.get("technicals")
    if not fetched_at or not isinstance(technicals, dict):
        return None
    try:
        ts = datetime.fromisoformat(fetched_at)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=KST)
        age = datetime.now(KST) - ts.astimezone(KST)
        if age <= timedelta(hours=max_age_hours):
            cached = dict(technicals)
            cached["cache_age_minutes"] = round(age.total_seconds() / 60, 1)
            cached["from_cache"] = True
            return cached
    except Exception:
        return None
    return None


def _store_cached_technicals(ticker: str, technicals: dict) -> None:
    cache = _load_technical_cache()
    payload = dict(technicals)
    payload.pop("from_cache", None)
    payload.pop("cache_age_minutes", None)
    cache[ticker] = {
        "fetched_at": datetime.now(KST).isoformat(),
        "technicals": payload,
    }
    _save_technical_cache(cache)


def _compute_technicals_from_history(history) -> dict | None:
    if history.empty or len(history) < 22:
        return None

    history.index = history.index.tz_localize(None)
    close = history["Close"]
    volume = history["Volume"]
    price = float(close.iloc[-1])
    if price <= 0:
        return None

    def _pct_change(days: int) -> float:
        if len(close) <= days:
            return 0.0
        prev_price = float(close.iloc[-days - 1])
        if prev_price == 0:
            return 0.0
        return round((price - prev_price) / prev_price * 100, 2)

    change_1d = _pct_change(1)
    change_3d = _pct_change(3)
    change_5d = _pct_change(5)

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, float("inf"))
    rsi_series = 100 - 100 / (1 + rs)
    rsi = float(rsi_series.iloc[-1])

    ma20 = float(close.rolling(20).mean().iloc[-1])
    ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else None

    std20 = float(close.rolling(20).std().iloc[-1])
    bb_upper = ma20 + 2 * std20
    bb_lower = ma20 - 2 * std20
    if bb_upper > bb_lower:
        bb_pos = round((price - bb_lower) / (bb_upper - bb_lower) * 100, 1)
    else:
        bb_pos = 50.0

    avg_vol = float(volume.iloc[-6:-1].mean()) if len(volume) >= 6 else float(volume.mean() or 1)
    vol_ratio = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

    rsi_divergence = False
    if len(close) >= 7 and change_5d < -1.5:
        price_5d = float(close.iloc[-6])
        rsi_5d = float(rsi_series.iloc[-6])
        rsi_divergence = price < price_5d and rsi > rsi_5d + 2

    if ma50:
        if price > ma20 > ma50:
            ma_alignment = "bullish"
        elif price < ma20 < ma50:
            ma_alignment = "bearish"
        else:
            ma_alignment = "neutral"
    else:
        ma_alignment = "neutral"

    bb_width = round((bb_upper - bb_lower) / ma20 * 100, 2) if ma20 > 0 else 0.0
    bb_width_3d_ago = None
    if len(close) >= 23:
        std_3d = float(close.rolling(20).std().iloc[-4])
        ma_3d = float(close.rolling(20).mean().iloc[-4])
        if ma_3d > 0:
            bb_width_3d_ago = ((ma_3d + 2 * std_3d) - (ma_3d - 2 * std_3d)) / ma_3d * 100
    bb_expanding = bb_width_3d_ago is not None and bb_width > bb_width_3d_ago * 1.05

    if len(volume) >= 10:
        vol_recent = float(volume.iloc[-5:].mean())
        vol_prior = float(volume.iloc[-10:-5].mean())
        vol_trend_5d = round((vol_recent - vol_prior) / vol_prior * 100, 1) if vol_prior > 0 else 0.0
    else:
        vol_trend_5d = 0.0
    vol_accumulation = change_5d < -2.0 and vol_trend_5d > 15

    high_52w = float(close.rolling(252).max().iloc[-1]) if len(close) >= 252 else float(close.max())
    low_52w = float(close.rolling(252).min().iloc[-1]) if len(close) >= 252 else float(close.min())
    if high_52w > low_52w:
        pos_52w = round((price - low_52w) / (high_52w - low_52w) * 100, 1)
    else:
        pos_52w = 50.0

    return {
        "price": round(price, 2),
        "change_1d": change_1d,
        "change_3d": change_3d,
        "change_5d": change_5d,
        "rsi": round(rsi, 1),
        "ma20": round(ma20, 2),
        "ma50": round(ma50, 2) if ma50 else None,
        "bb_pos": bb_pos,
        "vol_ratio": vol_ratio,
        "rsi_divergence": rsi_divergence,
        "52w_pos": pos_52w,
        "bb_width": bb_width,
        "bb_expanding": bb_expanding,
        "vol_trend_5d": vol_trend_5d,
        "vol_accumulation": vol_accumulation,
        "ma_alignment": ma_alignment,
        "from_cache": False,
        "cache_age_minutes": 0.0,
    }


def _is_rate_limit_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(token in text for token in ("too many requests", "rate limited", "429"))


def fetch_technicals(ticker: str) -> dict | None:
    """Compute the technical snapshot used by the JACKAL scanner."""
    last_exc: Exception | None = None
    for attempt in range(1, 4):
        try:
            history = yf.Ticker(ticker).history(period="1y", interval="1d")
            technicals = _compute_technicals_from_history(history)
            if technicals:
                _store_cached_technicals(ticker, technicals)
                return technicals
            if attempt == 1:
                cached = _load_cached_technicals(ticker, max_age_hours=TECHNICAL_CACHE_TTL_HOURS)
                if cached:
                    log.warning(
                        "%s technical fetch returned empty history; using %.1f min cache",
                        ticker,
                        cached.get("cache_age_minutes", 0.0),
                    )
                    return cached
            return None
        except Exception as exc:
            last_exc = exc
            if _is_rate_limit_error(exc) and attempt < 3:
                delay = round((1.2 * attempt) + random.uniform(0.2, 0.8), 2)
                log.warning("%s technical fetch rate-limited (attempt %s/3) — retry in %.2fs", ticker, attempt, delay)
                time.sleep(delay)
                continue
            break

    cached = _load_cached_technicals(ticker, max_age_hours=TECHNICAL_STALE_FALLBACK_HOURS)
    if cached:
        log.warning(
            "%s technical fetch failed: %s — using cached snapshot (%.1f min old)",
            ticker,
            last_exc,
            cached.get("cache_age_minutes", 0.0),
        )
        return cached

    log.error("%s technical fetch failed: %s", ticker, last_exc)
    return None


def fetch_all() -> dict:
    """Fetch the shared macro/context bundle used by the scanner."""
    fred = fetch_fred()
    krx = fetch_krx()
    fsc = fetch_fsc()
    sentiment = load_sentiment()

    log.info(
        "Macro bundle | FRED:%s KRX:%s FSC:%s Sentiment:%s",
        "ok" if fred.get("source") else "fallback",
        "ok" if krx.get("source") else "fallback",
        "ok" if fsc.get("source") else "fallback",
        sentiment.get("score", 50),
    )

    return {
        "fred": fred,
        "krx": krx,
        "fsc": fsc,
        "sentiment": sentiment,
    }
