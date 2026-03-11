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
from fastapi.responses import JSONResponse
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

# VN-Index: thử nhiều API vnstock (Quote, stock_historical_data, get_index_series)
try:
    from vnstock import Quote
except ImportError:
    Quote = None
try:
    from vnstock import stock_historical_data
except ImportError:
    stock_historical_data = None
try:
    from vnstock import get_index_series
except ImportError:
    get_index_series = None

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


def _get_vnindex_close_prices(days: int = 250) -> Optional[List[float]]:
    """Lấy chuỗi giá đóng cửa VN-Index. KBS hoạt động trên cloud (Render); VCI có thể bị chặn."""
    if days <= 0:
        days = 250

    def _extract_close(df) -> Optional[List[float]]:
        if df is None or (hasattr(df, "empty") and df.empty) or len(df) == 0:
            return None
        col_names = ("close", "Close", "indexValue", "index_value")
        for col_name in col_names:
            col = None
            if hasattr(df, "columns") and col_name in df.columns:
                col = df[col_name]
            elif hasattr(df, col_name):
                col = getattr(df, col_name)
            if col is not None:
                prices = _safe_float_list(col)
                if len(prices) >= 20:
                    return prices[-days:] if len(prices) > days else prices
        return None

    # 1) Quote.history - KBS/TCBS/DNSE (cloud), VCI (có thể bị chặn)
    if Quote is not None:
        for source in ("KBS", "TCBS", "DNSE", "VCI"):
            try:
                quote = Quote(symbol="VNINDEX", source=source)
                # length="1Y" ~ 252 phiên, đủ MA200
                df = quote.history(length="1Y", interval="1D")
                prices = _extract_close(df)
                if prices:
                    return prices
            except Exception:
                continue
        # Thử start/end nếu length không được hỗ trợ
        try:
            from datetime import date, timedelta
            end_d = date.today()
            start_d = end_d - timedelta(days=days * 2)
            quote = Quote(symbol="VNINDEX", source="KBS")
            df = quote.history(start=start_d.strftime("%Y-%m-%d"), end=end_d.strftime("%Y-%m-%d"), interval="1D")
            prices = _extract_close(df)
            if prices:
                return prices
        except Exception:
            pass

    # 2) get_index_series
    if get_index_series is not None:
        try:
            df = get_index_series(index_code="VNINDEX", time_range="OneYear")
            prices = _extract_close(df)
            if prices:
                return prices
        except Exception:
            pass

    # 3) stock_historical_data
    if stock_historical_data is not None:
        try:
            from datetime import date, timedelta
            end_d = date.today()
            start_d = end_d - timedelta(days=days * 2)
            start_str = start_d.strftime("%Y-%m-%d")
            end_str = end_d.strftime("%Y-%m-%d")
            for kwargs in [
                {"symbol": "VNINDEX", "start_date": start_str, "end_date": end_str, "type": "index"},
                {"symbol": "VNINDEX", "start_date": start_str, "end_date": end_str},
            ]:
                try:
                    df = stock_historical_data(**kwargs)
                    prices = _extract_close(df)
                    if prices:
                        return prices
                except TypeError:
                    try:
                        df = stock_historical_data("VNINDEX", start_str, end_str)
                        prices = _extract_close(df)
                        if prices:
                            return prices
                    except Exception:
                        pass
        except Exception:
            pass

    # 4) Fallback: Robotstock API (miễn phí 50 req/ngày, hoạt động từ server)
    try:
        import urllib.request
        rkey = os.environ.get("ROBOTSTOCK_API_KEY", "demo")
        url = f"https://api.robotstock.info.vn/api_data?type=his_stock&sym=VNINDEX&key={rkey}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; Fundamentals/1.0)"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            import json as _json
            rows = _json.loads(resp.read().decode())
        if isinstance(rows, list) and len(rows) >= 20:
            # Sắp xếp theo Date (YYYYMMDD), lấy Close. Robotstock trả giá * 50 (vd: 62975 = 1259.5)
            sorted_rows = sorted(rows, key=lambda r: r.get("Date", ""), reverse=True)
            prices = []
            for r in sorted_rows:
                c = r.get("Close") or r.get("close")
                if c is not None:
                    try:
                        v = float(c)
                        if v > 0 and v < 1e15:
                            prices.append(v)
                    except (TypeError, ValueError):
                        pass
            if len(prices) >= 20:
                prices = list(reversed(prices))  # cũ nhất -> mới nhất
                # Chuẩn hóa: nếu giá > 5000 thì chia 50 (format Robotstock)
                if max(prices) > 5000:
                    prices = [p / 50.0 for p in prices]
                return prices[-days:] if len(prices) > days else prices
    except Exception:
        pass

    # 5) Fallback: Yahoo Finance (^VNINDEX hoặc VNINDEX.VN)
    for yahoo_symbol in ("%5EVNINDEX", "VNINDEX.VN"):
        try:
            import urllib.request
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?interval=1d&range=1y"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; Vnstock/1.0)"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                import json as _json
                data = _json.loads(resp.read().decode())
            results = data.get("chart", {}).get("result") or []
            if not results:
                continue
            quote = results[0].get("indicators", {}).get("quote") or [{}]
            closes = quote[0].get("close") if quote else []
            if not closes:
                continue
            prices = [float(c) for c in closes if c is not None and isinstance(c, (int, float))]
            if len(prices) >= 20:
                return prices[-days:] if len(prices) > days else prices
        except Exception:
            continue

    return None


def _safe_float_list(seq: Any) -> List[float]:
    out: List[float] = []
    if seq is None:
        return out
    try:
        arr = seq.tolist() if hasattr(seq, "tolist") else list(seq)
    except Exception:
        return out
    for v in arr:
        try:
            x = float(v)
        except (TypeError, ValueError):
            continue
        if x == x and abs(x) < 1e15:
            out.append(x)
    return out


def _compute_vnindex_overview() -> Optional[Dict[str, Any]]:
    """Tính last, ma20, ma50, ma200 từ chuỗi giá VN-Index."""
    prices = _get_vnindex_close_prices(250)
    if not prices or len(prices) < 20:
        return None

    last = prices[-1]
    if last < 100 or last > 5000:
        return None

    def sma(arr: List[float], period: int) -> Optional[float]:
        if len(arr) < period:
            return None
        return sum(arr[-period:]) / period

    ma20_val = sma(prices, 20)
    ma50_val = sma(prices, 50)
    ma200_val = sma(prices, 200)
    return {
        "last": round(last, 2),
        "ma20": round(ma20_val, 2) if ma20_val is not None else None,
        "ma50": round(ma50_val, 2) if ma50_val is not None else None,
        "ma200": round(ma200_val, 2) if ma200_val is not None else None,
    }


@app.get("/api/vnindex-overview")
@app.get("/vnindex-overview")
def api_vnindex_overview():
    """
    GET VN-Index overview: last, MA(20), MA(50), MA(200).
    Dùng cho đánh giá xu hướng thị trường chung trong báo cáo phân tích.
    """
    result = _compute_vnindex_overview()
    if result is None:
        return JSONResponse(content={"error": "Không lấy được dữ liệu VN-Index"}, status_code=503)
    return JSONResponse(content=result)


@app.post("/api/fundamentals")
@app.post("/fundamentals")
@app.post("/")
def api_fundamentals(req: FundamentalsRequest):
    """
    FastAPI endpoint for fundamentals.

    Request body:
        {"tickers": ["SSI", "MBB", ...]}

    Response:
        {"data": {"SSI": {"pe", "pb", "roe", "eps"}, ...}}
    """
    tickers = req.tickers or []
    unique = list({str(t).strip().upper() for t in tickers if t})
    data: Dict[str, Dict[str, float]] = {}
    for symbol in unique:
        try:
            item = _extract_for_sources(symbol, SOURCES)
            if any(x is not None for x in item.values()):
                # Loại bỏ None để tránh ResponseValidationError
                data[symbol] = {k: v for k, v in item.items() if v is not None}
        except Exception:
            continue
    return JSONResponse(content={"data": data})


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
