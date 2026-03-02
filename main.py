"""
VN Fundamentals API – dùng vnstock (https://github.com/thinh-vu/vnstock) để lấy P/E, P/B, ROE, EPS.
Chạy: uvicorn main:app --reload --port 8001
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from pydantic import BaseModel

# Optional: đăng ký vnstock để tăng giới hạn (60 req/phút Community, không cần thì bỏ qua)
_api_key = os.environ.get("VNSTOCK_API_KEY")
if _api_key:
    try:
        from vnstock import register_user
        register_user(api_key=_api_key)
    except Exception:
        pass

from vnstock import Company, Finance


class FundamentalsRequest(BaseModel):
    tickers: List[str]


class FundamentalsItem(BaseModel):
    pe: Optional[float] = None
    pb: Optional[float] = None
    roe: Optional[float] = None
    eps: Optional[float] = None


class FundamentalsResponse(BaseModel):
    data: Dict[str, FundamentalsItem]


app = FastAPI(title="VN Fundamentals Service (vnstock)", version="2.0.0")

# Nguồn dữ liệu: KBS hoặc VCI (vnstock 3.x)
DEFAULT_SOURCE = "KBS"


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        x = float(value)
        return x if (x == x and abs(x) < 1e15) else None  # reject nan/inf
    except (TypeError, ValueError):
        return None


def _get_ratio_row(symbol: str, source: str) -> Optional[Dict[str, Any]]:
    """Lấy dòng chỉ số tài chính mới nhất từ Finance.ratio()."""
    try:
        finance = Finance(symbol=symbol, source=source)
        df = finance.ratio(period="year", lang="vi")
    except Exception:
        return None
    if df is None or df.empty:
        return None
    # Hàng mới nhất (năm gần nhất)
    row = df.iloc[-1]
    return row.to_dict() if hasattr(row, "to_dict") else dict(row)


def _get_overview_row(symbol: str, source: str) -> Optional[Dict[str, Any]]:
    """Lấy overview công ty (có thể có thêm pe, pb, ...)."""
    try:
        company = Company(symbol=symbol, source=source)
        df = company.overview()
    except Exception:
        return None
    if df is None or df.empty:
        return None
    row = df.iloc[-1]
    return row.to_dict() if hasattr(row, "to_dict") else dict(row)


def _extract_fundamentals(symbol: str, source: str) -> FundamentalsItem:
    """Gộp dữ liệu từ ratio (và overview nếu cần) thành pe, pb, roe, eps."""
    pe = pb = roe = eps = None

    ratio_row = _get_ratio_row(symbol, source)
    if ratio_row:
        # Cột có thể là tiếng Anh hoặc Việt tùy lang=vi
        for key in ("pe", "PE", "p_e", "P/E", "priceToEarning"):
            if key in ratio_row:
                pe = _safe_float(ratio_row[key])
                break
        for key in ("pb", "PB", "p_b", "P/B", "priceToBook"):
            if key in ratio_row:
                pb = _safe_float(ratio_row[key])
                break
        for key in ("roe", "ROE", "returnOnEquity"):
            if key in ratio_row:
                roe = _safe_float(ratio_row[key])
                break
        for key in ("eps", "EPS", "earningsPerShare"):
            if key in ratio_row:
                eps = _safe_float(ratio_row[key])
                break

    # Nếu ratio không đủ, thử overview (một số nguồn trả pe/pb ở overview)
    if (pe is None or pb is None) and ratio_row is None:
        overview_row = _get_overview_row(symbol, source)
        if overview_row:
            if pe is None and "pe" in overview_row:
                pe = _safe_float(overview_row["pe"])
            if pb is None and "pb" in overview_row:
                pb = _safe_float(overview_row["pb"])

    return FundamentalsItem(pe=pe, pb=pb, roe=roe, eps=eps)


@app.post("/fundamentals", response_model=FundamentalsResponse)
def get_fundamentals(payload: FundamentalsRequest) -> FundamentalsResponse:
    """Trả về P/E, P/B, ROE, EPS cho danh sách mã VN qua vnstock (Company + Finance)."""
    result: Dict[str, FundamentalsItem] = {}
    source = os.environ.get("VNSTOCK_SOURCE", DEFAULT_SOURCE)
    unique = list({t.strip().upper() for t in payload.tickers if t and t.strip()})

    for ticker in unique:
        try:
            item = _extract_fundamentals(ticker, source)
            # Chỉ thêm vào result nếu có ít nhất một chỉ số
            if any(x is not None for x in (item.pe, item.pb, item.roe, item.eps)):
                result[ticker] = item
        except Exception:
            continue

    return FundamentalsResponse(data=result)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
