"""
Fundamentals API: POST /api/fundamentals (or POST /)
Body: {"tickers": ["SSI", "MBB", ...]} -> {"data": {"SSI": {"pe", "pb", "roe", "eps"}, ...}}
Uses vnstock (Company, Finance) to get P/E, P/B, ROE, EPS.
Supports both Render (uvicorn main:app) and Vercel (handler).
"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Optional: tăng giới hạn vnstock (đăng ký tại https://vnstocks.com/login)
_api_key = os.environ.get("VNSTOCK_API_KEY")
if _api_key:
    try:
        from vnstock import register_user
        register_user(api_key=_api_key)
    except Exception:
        pass

from vnstock import Company, Finance

app = FastAPI(title="Fundamentals API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class FundamentalsRequest(BaseModel):
    tickers: List[str] = []


class FundamentalsItem(BaseModel):
    pe: Optional[float] = None
    pb: Optional[float] = None
    roe: Optional[float] = None
    eps: Optional[float] = None


class FundamentalsResponse(BaseModel):
    data: Dict[str, FundamentalsItem]

# Có thể cấu hình nhiều nguồn, phân tách bằng dấu phẩy, ví dụ: "KBS,SSI,CAFE"
_RAW_SOURCES = os.environ.get("VNSTOCK_SOURCE", "KBS,SSI,CAFE")
SOURCES = [s.strip() for s in _RAW_SOURCES.split(",") if s.strip()]


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        x = float(value)
        return x if (x == x and abs(x) < 1e15) else None
    except (TypeError, ValueError):
        return None


# Map item_id từ vnstock ratio() -> field output (pe, pb, roe, eps)
_ITEM_ID_TO_FIELD = {
    "pe": "pe", "pe_ratio": "pe", "p_e": "pe", "price_to_earning": "pe", "ty_le_pe": "pe",
    "pb": "pb", "pb_ratio": "pb", "p_b": "pb", "price_to_book": "pb", "ty_le_pb": "pb",
    "roe": "roe", "return_on_equity": "roe",
    "eps": "eps", "earnings_per_share": "eps", "loi_nhuan_tren_co_phieu": "eps",
}


def _get_ratio_df(symbol: str, source: str):
    """Lấy DataFrame ratio từ vnstock. Mỗi hàng = một chỉ số (PE, PB, ROE, EPS...)."""
    try:
        finance = Finance(symbol=symbol, source=source)
        try:
            df = finance.ratio(period="year", lang="vi")
        except TypeError:
            df = finance.ratio(period="year", display_mode="vi")
    except Exception:
        return None
    if df is None or df.empty:
        return None
    return df


def _parse_ratio_df(df) -> Dict[str, Optional[float]]:
    """Từ DataFrame ratio (mỗi hàng = một chỉ số), trích pe, pb, roe, eps từ cột kỳ mới nhất."""
    out: Dict[str, Optional[float]] = {"pe": None, "pb": None, "roe": None, "eps": None}
    meta = {"item", "item_id", "item_en", "unit", "levels", "row_number"}
    period_cols = [c for c in df.columns if c not in meta and str(c).strip()]
    if not period_cols:
        return out
    latest_col = period_cols[0]

    for _, row in df.iterrows():
        raw = (row.get("item_id") or row.get("item_en") or row.get("item")) or ""
        item_id = str(raw).strip().lower().replace(" ", "_").replace("-", "_")
        field = _ITEM_ID_TO_FIELD.get(item_id)
        if not field or field not in out:
            continue
        val = row.get(latest_col)
        if val is None and len(period_cols) > 1:
            val = row.get(period_cols[1])
        out[field] = _safe_float(val)
    return out


def _get_overview_row(symbol: str, source: str) -> Optional[Dict[str, Any]]:
    try:
        try:
            company = Company(symbol=symbol, source=source)
        except TypeError:
            company = Company(symbol=symbol)
        df = company.overview()
    except Exception:
        return None
    if df is None or df.empty:
        return None
    row = df.iloc[-1]
    return row.to_dict() if hasattr(row, "to_dict") else dict(row)


def _extract(symbol: str, source: str) -> Dict[str, Optional[float]]:
    pe = pb = roe = eps = None
    df = _get_ratio_df(symbol, source)
    if df is not None:
        out = _parse_ratio_df(df)
        pe, pb, roe, eps = out["pe"], out["pb"], out["roe"], out["eps"]
    if (pe is None or pb is None) and df is None:
        overview_row = _get_overview_row(symbol, source)
        if overview_row:
            if pe is None:
                pe = _safe_float(
                    overview_row.get("pe") or overview_row.get("pe_ratio") or overview_row.get("P/E")
                )
            if pb is None:
                pb = _safe_float(
                    overview_row.get("pb") or overview_row.get("pb_ratio") or overview_row.get("P/B")
                )
    return {"pe": pe, "pb": pb, "roe": roe, "eps": eps}


def _extract_for_sources(symbol: str, sources: List[str]) -> Dict[str, Optional[float]]:
    """Thử lần lượt nhiều nguồn vnstock cho tới khi lấy được ít nhất một chỉ số."""
    last_item: Dict[str, Optional[float]] = {
        "pe": None,
        "pb": None,
        "roe": None,
        "eps": None,
    }
    for src in sources:
        item = _extract(symbol, src)
        if any(x is not None for x in item.values()):
            return item
        last_item = item
    return last_item


@app.post("/api/fundamentals")
@app.post("/fundamentals")
@app.post("/")
def api_fundamentals(req: FundamentalsRequest) -> FundamentalsResponse:
    """
    FastAPI endpoint for fundamentals.

    Request body:
        {"tickers": ["SSI", "MBB", ...]}

    Response:
        {"data": {"SSI": {"pe", "pb", "roe", "eps"}, ...}}
    """
    tickers = req.tickers or []
    unique = list({str(t).strip().upper() for t in tickers if t})
    data: Dict[str, FundamentalsItem] = {}
    for symbol in unique:
        try:
            item = _extract_for_sources(symbol, SOURCES)
            if any(x is not None for x in item.values()):
                data[symbol] = FundamentalsItem(**item)
        except Exception:
            continue
    return FundamentalsResponse(data=data)


def handler(req: BaseHTTPRequestHandler):
    """Vercel gọi do_POST; req là self (BaseHTTPRequestHandler)."""
    pass


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
            payload = json.loads(body) if body.strip() else {}
            tickers = payload.get("tickers") or []
        except Exception:
            tickers = []

        unique = list({str(t).strip().upper() for t in tickers if t})
        data: Dict[str, Dict[str, Optional[float]]] = {}
        for symbol in unique:
            try:
                item = _extract_for_sources(symbol, SOURCES)
                if any(x is not None for x in item.values()):
                    data[symbol] = item
            except Exception:
                continue

        out = json.dumps({"data": data}, ensure_ascii=False)
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(out.encode("utf-8"))

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
