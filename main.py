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

# Khối ngoại / Tự doanh: optional vnstock_data (pip install from GitHub)
_Trading = None
try:
    from vnstock_data import Trading as _Trading
except Exception:
    # Fallback: một số bản vnstock expose Trading trực tiếp
    try:
        from vnstock import Trading as _Trading
    except Exception:
        pass

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

class MoneyflowRequest(BaseModel):
    tickers: List[str] = []
    # Số ngày gần nhất để tổng hợp giao dịch khối ngoại/tự doanh
    days: Optional[int] = 30

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

# Map item_id từ cash_flow() -> field (dòng tiền: hoạt động kinh doanh, tăng/giảm tiền thuần)
# KBS: dòng tiêu đề "I. Lưu chuyển tiền tệ..." có thể NaN; lấy dòng con có số (net_cash_flows_...)
_CASH_FLOW_ITEM_IDS = (
    "net_cash_flows_from_operating_activities",
    "net_cash_from_operating_activities",
    "luu_chuyen_tien_thuan_tu_hoat_dong_kinh_doanh",
    "cash_from_operations",
    "operating_activities",
)
_NET_CASH_ITEM_IDS = (
    "net_increase_decrease_in_cash_and_cash_equivalents",
    "net_increase_decrease_in_cash",
    "increase_decrease_in_cash_and_cash_equivalents",
    "tang_giam_thuan_tien_va_tuong_duong_tien",
)


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


def _get_cash_flow_df(symbol: str, source: str):
    """Lấy DataFrame báo cáo lưu chuyển tiền tệ (Finance.cash_flow)."""
    try:
        finance = Finance(symbol=symbol, source=source)
        try:
            df = finance.cash_flow(period="year", lang="vi")
        except TypeError:
            df = finance.cash_flow(period="year", display_mode="vi")
    except Exception:
        return None
    if df is None or df.empty:
        return None
    return df


def _parse_cash_flow_df(df) -> Dict[str, Optional[float]]:
    """Trích dòng tiền từ hoạt động kinh doanh và tăng/giảm tiền thuần từ cột kỳ mới nhất."""
    out: Dict[str, Optional[float]] = {
        "cash_flow_operating": None,
        "cash_flow_net": None,
    }
    meta = {"item", "item_id", "item_en", "unit", "levels", "row_number"}
    period_cols = [c for c in df.columns if c not in meta and str(c).strip()]
    if not period_cols:
        return out
    latest_col = period_cols[0]

    for _, row in df.iterrows():
        raw = (row.get("item_id") or row.get("item_en") or row.get("item")) or ""
        item_id = str(raw).strip().lower().replace(" ", "_").replace("-", "_")
        val = row.get(latest_col)
        if val is None and len(period_cols) > 1:
            val = row.get(period_cols[1])
        v = _safe_float(val)
        if v is None:
            continue
        if out["cash_flow_operating"] is None and any(x in item_id for x in _CASH_FLOW_ITEM_IDS):
            out["cash_flow_operating"] = v
        if out["cash_flow_net"] is None and any(x in item_id for x in _NET_CASH_ITEM_IDS):
            out["cash_flow_net"] = v
    return out


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


def _get_trading_flow(symbol: str, days: int = 30) -> Optional[Dict[str, Any]]:
    """
    Lấy dòng tiền khối ngoại và tự doanh (nếu có vnstock_data).
    Trả về: foreign_net_value, foreign_net_volume, proprietary_net_value, proprietary_net_volume.
    """
    if _Trading is None:
        return None
    try:
        from datetime import date, timedelta
        end_d = date.today()
        start_d = end_d - timedelta(days=days)
        start_str = start_d.strftime("%Y-%m-%d")
        end_str = end_d.strftime("%Y-%m-%d")
        trading = _Trading(symbol=symbol, source="vci")
        out: Dict[str, Any] = {}
        # Khối ngoại
        try:
            fr_df = trading.foreign_trade(start=start_str, end=end_str)
            if fr_df is not None and not (hasattr(fr_df, "empty") and fr_df.empty):
                if hasattr(fr_df, "sum"):
                    s = fr_df.sum()
                    out["foreign_net_value"] = _safe_float(s.get("fr_net_value"))
                    out["foreign_net_volume"] = _safe_float(s.get("fr_net_volume"))
                elif isinstance(fr_df, dict):
                    out["foreign_net_value"] = _safe_float(fr_df.get("fr_net_value"))
                    out["foreign_net_volume"] = _safe_float(fr_df.get("fr_net_volume"))
        except Exception:
            pass
        # Tự doanh
        try:
            prop_df = trading.prop_trade(start=start_str, end=end_str, resolution="1D")
            if prop_df is not None and not (hasattr(prop_df, "empty") and prop_df.empty):
                if hasattr(prop_df, "sum"):
                    s = prop_df.sum()
                    out["proprietary_net_value"] = _safe_float(
                        s.get("total_trade_net_value") or s.get("total_deal_trade_net_value")
                    )
                    out["proprietary_net_volume"] = _safe_float(
                        s.get("total_trade_net_volume") or s.get("total_deal_trade_net_volume")
                    )
                elif isinstance(prop_df, dict):
                    out["proprietary_net_value"] = _safe_float(
                        prop_df.get("total_trade_net_value") or prop_df.get("total_deal_trade_net_value")
                    )
                    out["proprietary_net_volume"] = _safe_float(
                        prop_df.get("total_trade_net_volume") or prop_df.get("total_deal_trade_net_volume")
                    )
        except Exception:
            pass
        return out if out else None
    except Exception:
        return None


def _get_moneyflow(symbol: str, days: int = 30) -> Optional[Dict[str, Optional[float]]]:
    """
    Lấy khối ngoại/tự doanh (mua/bán) trên một cửa sổ ngày gần nhất.

    Response fields:
      - foreignBuy, foreignSell (giá trị mua/bán ròng theo NĐTNN)
      - proprietaryBuy, proprietarySell (giá trị mua/bán theo tự doanh)
    """
    out: Dict[str, Optional[float]] = {
        "foreignBuy": None,
        "foreignSell": None,
        "proprietaryBuy": None,
        "proprietarySell": None,
        # Room ngoại
        "foreignRoomCurrent": None,
        "foreignRoomTotal": None,
        "foreignOwnership": None,
    }
    if _Trading is None:
        return out

    try:
        from datetime import date, timedelta

        end_d = date.today()
        start_d = end_d - timedelta(days=days)
        start_str = start_d.strftime("%Y-%m-%d")
        end_str = end_d.strftime("%Y-%m-%d")
        trading = _Trading(symbol=symbol, source="vci")

        # Khối ngoại
        fr_df = None
        try:
            fr_df = trading.foreign_trade(start=start_str, end=end_str)
        except Exception:
            fr_df = None

        if fr_df is not None and not (hasattr(fr_df, "empty") and fr_df.empty):
            if hasattr(fr_df, "columns"):
                if "fr_buy_value" in fr_df.columns:
                    out["foreignBuy"] = _safe_float(fr_df["fr_buy_value"].sum())
                if "fr_sell_value" in fr_df.columns:
                    out["foreignSell"] = _safe_float(fr_df["fr_sell_value"].sum())
                if "fr_current_room" in fr_df.columns:
                    out["foreignRoomCurrent"] = _safe_float(fr_df["fr_current_room"].iloc[-1])
                if "fr_total_room" in fr_df.columns:
                    out["foreignRoomTotal"] = _safe_float(fr_df["fr_total_room"].iloc[-1])
                if "fr_ownership" in fr_df.columns:
                    out["foreignOwnership"] = _safe_float(fr_df["fr_ownership"].iloc[-1])
            elif isinstance(fr_df, dict):
                out["foreignBuy"] = _safe_float(fr_df.get("fr_buy_value"))
                out["foreignSell"] = _safe_float(fr_df.get("fr_sell_value"))
                out["foreignRoomCurrent"] = _safe_float(fr_df.get("fr_current_room"))
                out["foreignRoomTotal"] = _safe_float(fr_df.get("fr_total_room"))
                out["foreignOwnership"] = _safe_float(fr_df.get("fr_ownership"))

        # Tự doanh
        prop_df = None
        try:
            prop_df = trading.prop_trade(start=start_str, end=end_str, resolution="1D")
        except Exception:
            prop_df = None

        if prop_df is not None and not (hasattr(prop_df, "empty") and prop_df.empty):
            # Ưu tiên cột tổng hợp theo "trade_value" (theo doc demo)
            buy_candidates = ("total_buy_trade_value", "total_deal_buy_trade_value")
            sell_candidates = ("total_sell_trade_value", "total_deal_sell_trade_value")

            if hasattr(prop_df, "columns"):
                for col in buy_candidates:
                    if col in prop_df.columns:
                        out["proprietaryBuy"] = _safe_float(prop_df[col].sum())
                        break
                for col in sell_candidates:
                    if col in prop_df.columns:
                        out["proprietarySell"] = _safe_float(prop_df[col].sum())
                        break
            elif isinstance(prop_df, dict):
                for col in buy_candidates:
                    v = prop_df.get(col)
                    if v is not None:
                        out["proprietaryBuy"] = _safe_float(v)
                        break
                for col in sell_candidates:
                    v = prop_df.get(col)
                    if v is not None:
                        out["proprietarySell"] = _safe_float(v)
                        break

        return out
    except Exception:
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


def _extract(symbol: str, source: str) -> Dict[str, Any]:
    pe = pb = roe = eps = None
    cash_flow_operating = cash_flow_net = None
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
    # Dòng tiền: báo cáo lưu chuyển tiền tệ (KBS/VCI)
    cf_df = _get_cash_flow_df(symbol, source)
    if cf_df is not None:
        cf_out = _parse_cash_flow_df(cf_df)
        cash_flow_operating = cf_out["cash_flow_operating"]
        cash_flow_net = cf_out["cash_flow_net"]
    result: Dict[str, Any] = {
        "pe": pe,
        "pb": pb,
        "roe": roe,
        "eps": eps,
    }
    if cash_flow_operating is not None or cash_flow_net is not None:
        result["cash_flow_operating"] = cash_flow_operating
        result["cash_flow_net"] = cash_flow_net
    # Khối ngoại & tự doanh (optional, cần vnstock_data)
    flow = _get_trading_flow(symbol)
    if flow:
        result["trading_flow"] = {k: v for k, v in flow.items() if v is not None}
    return result


def _extract_for_sources(symbol: str, sources: List[str]) -> Dict[str, Any]:
    """Thử lần lượt nhiều nguồn vnstock cho tới khi lấy được ít nhất một chỉ số."""
    last_item: Dict[str, Any] = {
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


def _normalize_vnindex_prices(prices: List[float]) -> List[float]:
    """Chuẩn hóa giá về thang VN-Index (100-5000). KBS trả nghìn (1.68=1680), Robotstock trả x50."""
    if not prices:
        return prices
    mx = max(prices)
    mn = min(prices)
    if mx < 100 and mn > 0.1:
        return [p * 1000.0 for p in prices]  # KBS: đơn vị nghìn
    if mx > 5000:
        return [p / 50.0 for p in prices]  # Robotstock: giá x50
    return prices


def _compute_vnindex_overview() -> Optional[Dict[str, Any]]:
    """Tính last, ma20, ma50, ma200 từ chuỗi giá VN-Index."""
    prices = _get_vnindex_close_prices(250)
    if not prices or len(prices) < 20:
        return None

    prices = _normalize_vnindex_prices(prices)
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


@app.post("/api/moneyflow")
@app.post("/moneyflow")
def api_moneyflow(req: MoneyflowRequest):
    """
    POST /api/moneyflow

    Request body:
      { "tickers": ["SSI", "MBB"], "days": 30 }

    Response:
      { "data": { "SSI": {
          "foreignBuy": number,
          "foreignSell": number,
          "proprietaryBuy": number,
          "proprietarySell": number
      }, ... } }
    """
    tickers = req.tickers or []
    days = req.days if req.days is not None else 30
    try:
        days_int = int(days)
    except Exception:
        days_int = 30
    days_int = days_int if days_int > 0 else 30

    unique = list({str(t).strip().upper() for t in tickers if t})
    data: Dict[str, Dict[str, Optional[float]]] = {}

    for symbol in unique:
        try:
            mf = _get_moneyflow(symbol, days=days_int)
            data[symbol] = mf or {
                "foreignBuy": None,
                "foreignSell": None,
                "proprietaryBuy": None,
                "proprietarySell": None,
                "foreignRoomCurrent": None,
                "foreignRoomTotal": None,
                "foreignOwnership": None,
            }
        except Exception:
            data[symbol] = {
                "foreignBuy": None,
                "foreignSell": None,
                "proprietaryBuy": None,
                "proprietarySell": None,
                "foreignRoomCurrent": None,
                "foreignRoomTotal": None,
                "foreignOwnership": None,
            }

    return JSONResponse(content={"data": data})


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
            days = payload.get("days", 30)
        except Exception:
            tickers = []
            days = 30

        unique = list({str(t).strip().upper() for t in tickers if t})
        path = (getattr(self, "path", "") or "").lower()

        # Vercel/compat mode: hỗ trợ /api/moneyflow và /moneyflow
        if "moneyflow" in path:
            try:
                days_int = int(days)
            except Exception:
                days_int = 30
            days_int = days_int if days_int > 0 else 30

            moneyflow: Dict[str, Dict[str, Optional[float]]] = {}
            for symbol in unique:
                try:
                    mf = _get_moneyflow(symbol, days=days_int)
                    moneyflow[symbol] = mf or {
                        "foreignBuy": None,
                        "foreignSell": None,
                        "proprietaryBuy": None,
                        "proprietarySell": None,
                        "foreignRoomCurrent": None,
                        "foreignRoomTotal": None,
                        "foreignOwnership": None,
                    }
                except Exception:
                    moneyflow[symbol] = {
                        "foreignBuy": None,
                        "foreignSell": None,
                        "proprietaryBuy": None,
                        "proprietarySell": None,
                        "foreignRoomCurrent": None,
                        "foreignRoomTotal": None,
                        "foreignOwnership": None,
                    }

            out = json.dumps({"data": moneyflow}, ensure_ascii=False)
        else:
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
