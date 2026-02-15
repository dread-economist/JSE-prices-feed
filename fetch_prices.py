#!/usr/bin/env python3
"""
Fetch JSE prices from the official Daily Quote Sheet PDF and write prices.csv

Output CSV format (for Google Sheets CSV_URL):
symbol,last_price,as_at,source

Notes
- Uses Playwright's HTTP client (APIRequestContext) so we don't trigger "download is starting"
- Does NOT launch a browser (no Chromium install needed)
- Tries multiple market ids + looks back several days to find the latest available sheet
"""

from __future__ import annotations

import io
import os
import re
import sys
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import pdfplumber
from playwright.sync_api import sync_playwright

BASE_PAGE = "https://www.jamstockex.com/trading/trade-quotes/daily-quote-pdf/"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36"

MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b"
)
PDF_HREF_RE = re.compile(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', re.IGNORECASE)
NUM_RE = re.compile(r"^[+-]?\d[\d,]*\.?\d*$")


@dataclass
class Quote:
    symbol: str
    last_price: Optional[float] = None
    as_at: str = ""
    source: str = "JSE_DAILY_PDF"


def read_watchlist(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        return []
    syms: List[str] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        syms.append(s.upper())
    return sorted(set(syms))


def parse_sheet_date(text: str) -> str:
    m = MONTH_DATE_RE.search(text)
    return m.group(0) if m else ""


def extract_pdf_link(html: str) -> Optional[str]:
    m = PDF_HREF_RE.search(html)
    return m.group(1) if m else None


def fetch_pdf(date_iso: str, market_id: int) -> bytes:
    url = f"{BASE_PAGE}?date={date_iso}&market={market_id}"
    with sync_playwright() as p:
        req = p.request.new_context(
            extra_http_headers={
                "User-Agent": UA,
                "Accept": "application/pdf,text/html;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        resp = req.get(url, timeout=90_000)
        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status} fetching quote page")

        body = resp.body()
        ctype = (resp.headers.get("content-type") or "").lower()

        # Direct PDF response
        if "pdf" in ctype or body[:4] == b"%PDF":
            return body

        # HTML wrapper: find embedded pdf link and fetch
        html = body.decode("utf-8", errors="ignore")
        rel_pdf = extract_pdf_link(html)
        if not rel_pdf:
            raise RuntimeError("Could not find PDF link in HTML wrapper page")

        pdf_url = urljoin(url, rel_pdf)
        resp2 = req.get(pdf_url, timeout=90_000)
        if not resp2.ok:
            raise RuntimeError(f"HTTP {resp2.status} fetching PDF link")
        pdf_body = resp2.body()
        if pdf_body[:4] != b"%PDF":
            raise RuntimeError("Fetched content is not a PDF")
        return pdf_body


def norm_sym(tok: str) -> str:
    # keep internal dots (10.00), strip trailing punctuation
    return tok.strip().upper().rstrip(".,;")


def parse_quotes(pdf_bytes: bytes, symbols: List[str]) -> Tuple[Dict[str, Quote], str]:
    want = set(symbols)
    out: Dict[str, Quote] = {}

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        texts: List[str] = []
        for pg in pdf.pages:
            texts.append(pg.extract_text() or "")

    full_text = "\n".join(texts)
    as_at = parse_sheet_date(full_text)

    # line-based parse: more reliable for tables
    for line in full_text.splitlines():
        if not line.strip():
            continue
        toks = [t for t in re.split(r"\s+", line.strip()) if t]
        if not toks:
            continue

        # symbol is often the first token
        sym0 = norm_sym(toks[0])
        idx0 = 0

        if sym0 not in want or sym0 in out:
            # sometimes symbol appears later in the line
            found = False
            for i, t in enumerate(toks[1:], start=1):
                sym = norm_sym(t)
                if sym in want and sym not in out:
                    sym0 = sym
                    idx0 = i
                    found = True
                    break
            if not found:
                continue

        # collect numeric tokens after the symbol; quote sheets usually have:
        # LastTraded, Close, Change, Bid, Ask, Volume, ...
        nums: List[str] = []
        for t in toks[idx0 + 1 :]:
            if NUM_RE.match(t):
                nums.append(t)

        def f(i: int) -> Optional[float]:
            if i >= len(nums):
                return None
            try:
                return float(nums[i].replace(",", ""))
            except ValueError:
                return None

        last_traded = f(0)
        close = f(1)
        price = close if close is not None else last_traded

        out[sym0] = Quote(symbol=sym0, last_price=price, as_at=as_at, source="JSE_DAILY_PDF")

        if len(out) == len(want):
            break

    return out, as_at


def latest_trading_date_iso(lookback_days: int) -> List[str]:
    try:
        from zoneinfo import ZoneInfo
        today = dt.datetime.now(ZoneInfo("America/Jamaica")).date()
    except Exception:
        today = dt.date.today()
    return [(today - dt.timedelta(days=d)).isoformat() for d in range(0, lookback_days)]


def main() -> int:
    symbols = read_watchlist(os.getenv("WATCHLIST_FILE", "watchlist.txt"))
    if not symbols:
        print("No symbols in watchlist.txt", file=sys.stderr)
        return 2

    # 31 is confirmed Main Market; the others are tried as fallbacks
    market_ids: List[int] = []
    for part in os.getenv("JSE_MARKETS", "33,31,32,34,35,36").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            market_ids.append(int(part))
        except ValueError:
            pass
    if not market_ids:
        market_ids = [31]

    lookback = int(os.getenv("JSE_LOOKBACK_DAYS", "10"))

    errors: List[str] = []
    best_quotes: Dict[str, Quote] = {}
    used_date = ""
    used_as_at = ""

    for date_iso in latest_trading_date_iso(lookback):
        combined: Dict[str, Quote] = {}
        got_any = False

        for mid in market_ids:
            try:
                pdf_bytes = fetch_pdf(date_iso, mid)
                parsed, as_at = parse_quotes(pdf_bytes, symbols)
                if parsed:
                    got_any = True
                    if as_at:
                        used_as_at = as_at
                    for k, v in parsed.items():
                        if k not in combined:
                            combined[k] = v
                if len(combined) == len(symbols):
                    best_quotes = combined
                    used_date = date_iso
                    break
            except Exception as e:
                errors.append(f"{date_iso} market {mid}: {type(e).__name__}: {e}")

        if best_quotes:
            break
        if got_any and combined:
            best_quotes = combined
            used_date = date_iso
            break

    rows = []
    for sym in symbols:
        q = best_quotes.get(sym)
        rows.append({
            "symbol": sym,
            "last_price": (q.last_price if q and q.last_price is not None else ""),
            "as_at": (q.as_at if q and q.as_at else used_as_at or used_date),
            "source": "JSE_DAILY_PDF",
        })

    pd.DataFrame(rows, columns=["symbol", "last_price", "as_at", "source"]).to_csv("prices.csv", index=False)
    print(f"Wrote prices.csv for {len(symbols)} symbols (date_try={used_date or 'n/a'}).")

    if os.getenv("DEBUG", "0") == "1" and errors:
        print("Errors (first 10):", file=sys.stderr)
        for e in errors[:10]:
            print(" -", e, file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
