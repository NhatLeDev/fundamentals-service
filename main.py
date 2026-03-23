"""
Fundamentals API: POST /api/fundamentals (or POST /)
Body: {"tickers": ["SSI", "MBB", ...]} -> {"data": {"SSI": {"pe", "pb", "roe", "eps"}, ...}}
Uses vnstock (Company, Finance) to get P/E, P/B, ROE, EPS.
Supports both Render (uvicorn main:app) and Vercel (handler).
"""
from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler
from threading import Lock
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
try:
    from vnstock import Listing
except ImportError:
    Listing = None

# Danh sách dự phòng khi VCI/Listing không gọi được (VN30 đổi định kỳ — ưu tiên API).
_VN30_FALLBACK_SYMBOLS = (
    "ACB",
    "BCM",
    "BID",
    "BVH",
    "CTG",
    "FPT",
    "GAS",
    "GVR",
    "HDB",
    "HPG",
    "KDH",
    "MBB",
    "MSN",
    "MWG",
    "NVL",
    "PDR",
    "PLX",
    "POW",
    "SAB",
    "SSI",
    "STB",
    "TCB",
    "TPB",
    "VCB",
    "VHM",
    "VIC",
    "VJC",
    "VNM",
    "VPB",
    "VRE",
)

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

# Mặc định tắt field "nặng" để giảm số request/ticker khi dùng Guest plan.
# Có thể bật lại bằng FUNDAMENTALS_ENABLE_HEAVY_FIELDS=1.
_ENABLE_HEAVY_FIELDS = (
    str(os.environ.get("FUNDAMENTALS_ENABLE_HEAVY_FIELDS", "0")).strip().lower()
    in ("1", "true", "yes", "on")
)

_FUNDAMENTALS_CACHE_TTL_SECONDS = max(
    10, int(os.environ.get("FUNDAMENTALS_CACHE_TTL_SECONDS", "300"))
)
_MONEYFLOW_CACHE_TTL_SECONDS = max(
    10, int(os.environ.get("MONEYFLOW_CACHE_TTL_SECONDS", "120"))
)
_cache_lock = Lock()
_fundamentals_cache: Dict[str, Dict[str, Any]] = {}
_moneyflow_cache: Dict[str, Dict[str, Any]] = {}


def _cache_get(cache: Dict[str, Dict[str, Any]], key: str) -> Optional[Any]:
    now = time.time()
    with _cache_lock:
        entry = cache.get(key)
        if not entry:
            return None
        if entry.get("expires_at", 0) <= now:
            try:
                del cache[key]
            except KeyError:
                pass
            return None
        return entry.get("value")


def _cache_set(cache: Dict[str, Dict[str, Any]], key: str, value: Any, ttl_seconds: int) -> None:
    with _cache_lock:
        cache[key] = {"value": value, "expires_at": time.time() + max(1, ttl_seconds)}


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
        
        # Try multiple sources if VCI fails
        sources = ["vci", "tcbs", "ssi"]
        trading = None
        
        for src in sources:
            try:
                trading = _Trading(symbol=symbol, source=src)
                break
            except Exception:
                continue
        
        if trading is None:
            return out

        # Khối ngoại
        fr_df = None
        try:
            fr_df = trading.foreign_trade(start=start_str, end=end_str)
        except Exception as e:
            # Try alternative method if available
            try:
                fr_df = trading.foreign_trading(start=start_str, end=end_str)
            except Exception:
                fr_df = None

        if fr_df is not None and not (hasattr(fr_df, "empty") and fr_df.empty):
            if hasattr(fr_df, "columns"):
                # Try multiple column name variations
                buy_cols = ["fr_buy_value", "foreign_buy_value", "buy_value"]
                sell_cols = ["fr_sell_value", "foreign_sell_value", "sell_value"]
                
                for col in buy_cols:
                    if col in fr_df.columns:
                        out["foreignBuy"] = _safe_float(fr_df[col].sum())
                        break
                
                for col in sell_cols:
                    if col in fr_df.columns:
                        out["foreignSell"] = _safe_float(fr_df[col].sum())
                        break
                
                if "fr_current_room" in fr_df.columns:
                    out["foreignRoomCurrent"] = _safe_float(fr_df["fr_current_room"].iloc[-1])
                if "fr_total_room" in fr_df.columns:
                    out["foreignRoomTotal"] = _safe_float(fr_df["fr_total_room"].iloc[-1])
                if "fr_ownership" in fr_df.columns:
                    out["foreignOwnership"] = _safe_float(fr_df["fr_ownership"].iloc[-1])
            elif isinstance(fr_df, dict):
                out["foreignBuy"] = _safe_float(fr_df.get("fr_buy_value") or fr_df.get("foreign_buy_value"))
                out["foreignSell"] = _safe_float(fr_df.get("fr_sell_value") or fr_df.get("foreign_sell_value"))
                out["foreignRoomCurrent"] = _safe_float(fr_df.get("fr_current_room"))
                out["foreignRoomTotal"] = _safe_float(fr_df.get("fr_total_room"))
                out["foreignOwnership"] = _safe_float(fr_df.get("fr_ownership"))

        # Tự doanh
        prop_df = None
        try:
            prop_df = trading.prop_trade(start=start_str, end=end_str, resolution="1D")
        except Exception:
            # Try alternative method
            try:
                prop_df = trading.proprietary_trade(start=start_str, end=end_str)
            except Exception:
                prop_df = None

        if prop_df is not None and not (hasattr(prop_df, "empty") and prop_df.empty):
            # Ưu tiên cột tổng hợp theo "trade_value" (theo doc demo)
            buy_candidates = ("total_buy_trade_value", "total_deal_buy_trade_value", "buy_value", "prop_buy_value")
            sell_candidates = ("total_sell_trade_value", "total_deal_sell_trade_value", "sell_value", "prop_sell_value")

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
    volume_ma20 = volume_ma50 = None
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
    if _ENABLE_HEAVY_FIELDS:
        # Khối ngoại & tự doanh (optional, cần vnstock_data)
        flow = _get_trading_flow(symbol)
        if flow:
            result["trading_flow"] = {k: v for k, v in flow.items() if v is not None}
        # Volume MA20/MA50 từ lịch sử khối lượng giao dịch
        volume_ma = _get_symbol_volume_ma(symbol, preferred_source=source)
        if volume_ma:
            volume_ma20 = volume_ma.get("volume_ma20")
            volume_ma50 = volume_ma.get("volume_ma50")
    if volume_ma20 is not None:
        result["volume_ma20"] = volume_ma20
    if volume_ma50 is not None:
        result["volume_ma50"] = volume_ma50
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


def _get_symbol_volume_ma(symbol: str, preferred_source: str, days: int = 120) -> Optional[Dict[str, float]]:
    """
    Tính MA20/MA50 khối lượng giao dịch cho một mã.
    Trả về: {"volume_ma20": ..., "volume_ma50": ...}
    """
    if Quote is None:
        return None

    src_candidates = [preferred_source, "KBS", "TCBS", "DNSE", "VCI"]
    tried: List[str] = []
    sources = []
    for s in src_candidates:
        ss = str(s or "").strip()
        if not ss or ss in tried:
            continue
        tried.append(ss)
        sources.append(ss)

    def _extract_volume(df) -> Optional[List[float]]:
        if df is None or (hasattr(df, "empty") and df.empty) or len(df) == 0:
            return None
        for col_name in ("volume", "Volume"):
            col = None
            if hasattr(df, "columns") and col_name in df.columns:
                col = df[col_name]
            elif hasattr(df, col_name):
                col = getattr(df, col_name)
            if col is not None:
                vols = _safe_float_list(col)
                if len(vols) >= 50:
                    return vols[-days:] if len(vols) > days else vols
        return None

    for src in sources:
        try:
            quote = Quote(symbol=symbol, source=src)
            df = quote.history(length="1Y", interval="1D")
            vols = _extract_volume(df)
            if not vols:
                continue
            if len(vols) < 50:
                continue
            ma20 = sum(vols[-20:]) / 20.0
            ma50 = sum(vols[-50:]) / 50.0
            return {
                "volume_ma20": round(ma20, 2),
                "volume_ma50": round(ma50, 2),
            }
        except Exception:
            continue
    return None


def _get_vnindex_bars(days: int = 260) -> Optional[List[Dict[str, float]]]:
    """Chuỗi nến daily VN-Index (cũ → mới): close, volume (0 nếu nguồn không có)."""
    if days <= 0:
        days = 260

    def _bars_from_df(df) -> Optional[List[Dict[str, float]]]:
        if df is None or (hasattr(df, "empty") and df.empty) or len(df) == 0:
            return None
        close_series = None
        for col_name in ("close", "Close", "indexValue", "index_value"):
            if hasattr(df, "columns") and col_name in df.columns:
                close_series = df[col_name]
                break
        if close_series is None:
            return None
        closes = _safe_float_list(close_series)
        if len(closes) < 20:
            return None
        vol_series = None
        for col_name in ("volume", "Volume", "totalVolume", "total_volume"):
            if hasattr(df, "columns") and col_name in df.columns:
                vol_series = df[col_name]
                break
        if vol_series is None:
            vols = [0.0] * len(closes)
        else:
            vols = _safe_float_list(vol_series)
            if len(vols) != len(closes):
                vols = [0.0] * len(closes)
        out = [{"close": closes[i], "volume": vols[i]} for i in range(len(closes))]
        return out[-days:] if len(out) > days else out

    # 1) Quote.history - KBS/TCBS/DNSE (cloud), VCI (có thể bị chặn)
    if Quote is not None:
        for source in ("KBS", "TCBS", "DNSE", "VCI"):
            try:
                quote = Quote(symbol="VNINDEX", source=source)
                df = quote.history(length="1Y", interval="1D")
                bars = _bars_from_df(df)
                if bars:
                    return bars
            except Exception:
                continue
        try:
            from datetime import date, timedelta

            end_d = date.today()
            start_d = end_d - timedelta(days=days * 2)
            quote = Quote(symbol="VNINDEX", source="KBS")
            df = quote.history(
                start=start_d.strftime("%Y-%m-%d"),
                end=end_d.strftime("%Y-%m-%d"),
                interval="1D",
            )
            bars = _bars_from_df(df)
            if bars:
                return bars
        except Exception:
            pass

    # 2) get_index_series
    if get_index_series is not None:
        try:
            df = get_index_series(index_code="VNINDEX", time_range="OneYear")
            bars = _bars_from_df(df)
            if bars:
                return bars
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
                    bars = _bars_from_df(df)
                    if bars:
                        return bars
                except TypeError:
                    try:
                        df = stock_historical_data("VNINDEX", start_str, end_str)
                        bars = _bars_from_df(df)
                        if bars:
                            return bars
                    except Exception:
                        pass
        except Exception:
            pass

    # 4) Robotstock
    try:
        import urllib.request

        rkey = os.environ.get("ROBOTSTOCK_API_KEY", "demo")
        url = f"https://api.robotstock.info.vn/api_data?type=his_stock&sym=VNINDEX&key={rkey}"
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (compatible; Fundamentals/1.0)"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            import json as _json

            rows = _json.loads(resp.read().decode())
        if isinstance(rows, list) and len(rows) >= 20:
            sorted_rows = sorted(rows, key=lambda r: r.get("Date", ""))
            bars_rs: List[Dict[str, float]] = []
            for r in sorted_rows:
                c = r.get("Close") or r.get("close")
                vol_raw = r.get("Volume") or r.get("volume") or r.get("Vol") or 0
                if c is None:
                    continue
                try:
                    close_v = float(c)
                    vol_v = float(vol_raw) if vol_raw not in (None, "") else 0.0
                except (TypeError, ValueError):
                    continue
                if close_v > 0 and close_v < 1e15:
                    bars_rs.append({"close": close_v, "volume": vol_v})
            if len(bars_rs) >= 20:
                if max(b["close"] for b in bars_rs) > 5000:
                    for b in bars_rs:
                        b["close"] /= 50.0
                return bars_rs[-days:] if len(bars_rs) > days else bars_rs
    except Exception:
        pass

    # 5) Yahoo Finance
    for yahoo_symbol in ("%5EVNINDEX", "VNINDEX.VN"):
        try:
            import urllib.request

            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?interval=1d&range=1y"
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0 (compatible; Vnstock/1.0)"}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                import json as _json

                data = _json.loads(resp.read().decode())
            results = data.get("chart", {}).get("result") or []
            if not results:
                continue
            quote = results[0].get("indicators", {}).get("quote") or [{}]
            q0 = quote[0] if quote else {}
            closes_raw = q0.get("close") or []
            vols_raw = q0.get("volume") or []
            if not closes_raw:
                continue
            bars_y: List[Dict[str, float]] = []
            for i, c in enumerate(closes_raw):
                if c is None or not isinstance(c, (int, float)):
                    continue
                vv = (
                    float(vols_raw[i])
                    if i < len(vols_raw) and isinstance(vols_raw[i], (int, float))
                    else 0.0
                )
                bars_y.append({"close": float(c), "volume": vv})
            if len(bars_y) >= 20:
                return bars_y[-days:] if len(bars_y) > days else bars_y
        except Exception:
            continue

    return None


def _get_vnindex_close_prices(days: int = 250) -> Optional[List[float]]:
    """Lấy chuỗi giá đóng cửa VN-Index. KBS hoạt động trên cloud (Render); VCI có thể bị chặn."""
    bars = _get_vnindex_bars(days)
    if not bars:
        return None
    return [b["close"] for b in bars]


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


def _normalize_vnindex_bars(bars: List[Dict[str, float]]) -> List[Dict[str, float]]:
    closes = [b["close"] for b in bars]
    closes_n = _normalize_vnindex_prices(closes)
    return [
        {"close": closes_n[i], "volume": bars[i]["volume"]}
        for i in range(len(bars))
    ]


def _compute_rsi14(closes: List[float]) -> Optional[float]:
    period = 14
    if len(closes) < period + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss <= 1e-12:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(rsi, 2)


def _ma200_streak_sessions(closes: List[float]) -> Dict[str, Any]:
    """
    Đếm số phiên liên tiếp gần nhất đóng cửa dưới / trên MA200 (MA200 rolling theo từng phiên).
    Trả về streak_below, streak_above (một trong hai = 0).
    """
    n = len(closes)
    below = 0
    above = 0
    for j in range(n - 1, -1, -1):
        hist = closes[: j + 1]
        if len(hist) < 200:
            break
        ma200_j = sum(hist[-200:]) / 200.0
        cj = closes[j]
        if cj < ma200_j:
            if above > 0:
                break
            below += 1
        elif cj > ma200_j:
            if below > 0:
                break
            above += 1
        else:
            break
    return {"streak_below_ma200": below, "streak_above_ma200": above}


def _build_market_phase_label(
    last: float,
    ma20_val: Optional[float],
    ma50_val: Optional[float],
    ma200_val: Optional[float],
    streak_below: int,
    streak_above: int,
) -> str:
    parts: List[str] = []
    if ma200_val is not None:
        if last < ma200_val:
            parts.append("DOWNTREND (giá dưới MA200)")
        elif last > ma200_val:
            parts.append("UPTREND (giá trên MA200)")
        else:
            parts.append("Giá tại MA200 (điểm cân bằng)")
    else:
        parts.append("Pha thị trường: MA200 chưa đủ dữ liệu")

    if ma20_val is not None and ma50_val is not None and ma200_val is not None:
        if ma20_val > ma50_val > ma200_val and last > ma200_val:
            parts.append("cấu trúc MA20>MA50>MA200")
        elif ma20_val < ma50_val < ma200_val and last < ma200_val:
            parts.append("cấu trúc MA20<MA50<MA200")

    if streak_below > 0:
        parts.append(f"— Phiên thứ {streak_below} liên tiếp đóng cửa dưới MA200")
    elif streak_above > 0:
        parts.append(f"— Phiên thứ {streak_above} liên tiếp đóng cửa trên MA200")

    return " ".join(parts)


def _vn30_symbol_list() -> List[str]:
    if Listing is not None:
        try:
            series = Listing().symbols_by_group("VN30")
            raw = series.tolist() if hasattr(series, "tolist") else list(series)
            out = [str(x).strip().upper() for x in raw if x]
            out = list(dict.fromkeys(out))
            if len(out) >= 25:
                return out[:35]
        except Exception:
            pass
    return list(_VN30_FALLBACK_SYMBOLS)


def _get_equity_close_prices(symbol: str, days: int = 220) -> Optional[List[float]]:
    if Quote is None:
        return None
    for source in ("KBS", "TCBS", "DNSE", "VCI"):
        try:
            quote = Quote(symbol=symbol, source=source)
            df = quote.history(length="1Y", interval="1D")
            if df is None or (hasattr(df, "empty") and df.empty):
                continue
            for col_name in ("close", "Close"):
                if hasattr(df, "columns") and col_name in df.columns:
                    prices = _safe_float_list(df[col_name])
                    if len(prices) >= 200:
                        return prices[-days:] if len(prices) > days else prices
                    break
        except Exception:
            continue
    return None


def _vn30_one_above_ma200(sym: str) -> Optional[bool]:
    closes = _get_equity_close_prices(sym, 220)
    if not closes or len(closes) < 200:
        return None
    last_c = closes[-1]
    ma200 = sum(closes[-200:]) / 200.0
    return last_c > ma200


def _compute_vn30_above_ma200_breadth() -> Dict[str, Any]:
    symbols = _vn30_symbol_list()
    above = 0
    total = 0
    failed = 0
    workers = min(10, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_vn30_one_above_ma200, symbols))
    for ok in results:
        if ok is None:
            failed += 1
        else:
            total += 1
            if ok:
                above += 1
    pct: Optional[float] = None
    if total > 0:
        pct = round(100.0 * above / total, 2)
    return {
        "vn30AboveMa200Pct": pct,
        "vn30AboveMa200Count": above,
        "vn30BreadthSampleSize": total,
        "vn30BreadthFailedFetch": failed,
    }


def _volume_today_vs_avg20(bars: List[Dict[str, float]]) -> Dict[str, Any]:
    vols = [b.get("volume") or 0.0 for b in bars]
    if len(vols) < 21:
        return {
            "volume_today": None,
            "volume_avg20": None,
            "volume_vs_avg20_pct": None,
        }
    today_v = vols[-1]
    prev20 = vols[-21:-1]
    avg20 = sum(prev20) / len(prev20) if prev20 else None
    if avg20 is not None and avg20 <= 0:
        avg20 = None
    vs_pct: Optional[float] = None
    if avg20 is not None and avg20 > 0 and today_v > 0:
        vs_pct = round((today_v / avg20 - 1.0) * 100.0, 2)
    return {
        "volume_today": round(today_v, 2) if today_v > 0 else None,
        "volume_avg20": round(avg20, 2) if avg20 is not None else None,
        "volume_vs_avg20_pct": vs_pct,
    }


def _compute_vnindex_overview() -> Optional[Dict[str, Any]]:
    """Tính last, MA, RSI, thanh khoản, pha thị trường, breadth VN30/MA200."""
    bars = _get_vnindex_bars(260)
    if not bars or len(bars) < 20:
        return None

    bars = _normalize_vnindex_bars(bars)
    prices = [b["close"] for b in bars]
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
    rsi14 = _compute_rsi14(prices)
    streak_info = _ma200_streak_sessions(prices)
    sb = int(streak_info["streak_below_ma200"])
    sa = int(streak_info["streak_above_ma200"])
    phase_label = _build_market_phase_label(
        last, ma20_val, ma50_val, ma200_val, sb, sa
    )
    vol_info = _volume_today_vs_avg20(bars)

    breadth: Dict[str, Any] = {}
    try:
        breadth = _compute_vn30_above_ma200_breadth()
    except Exception:
        breadth = {
            "vn30AboveMa200Pct": None,
            "vn30AboveMa200Count": None,
            "vn30BreadthSampleSize": None,
            "vn30BreadthFailedFetch": None,
        }

    out: Dict[str, Any] = {
        "last": round(last, 2),
        "ma20": round(ma20_val, 2) if ma20_val is not None else None,
        "ma50": round(ma50_val, 2) if ma50_val is not None else None,
        "ma200": round(ma200_val, 2) if ma200_val is not None else None,
        "rsi14": rsi14,
        "marketPhaseLabel": phase_label,
        "streakBelowMa200": sb,
        "streakAboveMa200": sa,
        "volumeToday": vol_info["volume_today"],
        "volumeAvg20": vol_info["volume_avg20"],
        "volumeVsAvg20Pct": vol_info["volume_vs_avg20_pct"],
    }
    out.update(breadth)
    return out


@app.get("/health")
@app.get("/api/health")
def health_check():
    """Health check endpoint for monitoring service status."""
    from datetime import datetime
    return JSONResponse(content={
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "service": "fundamentals-api",
        "version": "1.0.0",
        "vnstock_available": True,
        "trading_available": _Trading is not None,
    })


@app.get("/api/vnindex-overview")
@app.get("/vnindex-overview")
def api_vnindex_overview():
    """
    GET VN-Index overview: last, MA(20/50/200), RSI14, pha thị trường (nhãn + streak MA200),
    thanh khoản (20 phiên vs phiên hiện tại), % mẫu VN30 trên MA200.
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
    
    # Add metadata for debugging
    metadata = {
        "requested_tickers": unique,
        "days": days_int,
        "trading_available": _Trading is not None,
    }

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
        except Exception as e:
            # Log error but continue with other symbols
            print(f"Error fetching moneyflow for {symbol}: {str(e)}")
            data[symbol] = {
                "foreignBuy": None,
                "foreignSell": None,
                "proprietaryBuy": None,
                "proprietarySell": None,
                "foreignRoomCurrent": None,
                "foreignRoomTotal": None,
                "foreignOwnership": None,
            }

    return JSONResponse(content={"data": data, "_debug": metadata})
class MoneyFlowRequest(BaseModel):
    tickers: List[str] = []
    # Tổng hợp KN/TD theo số ngày lịch gần nhất.
    # Front-end hiện tại đang gọi không truyền tham số này nên mặc định = 30.
    totalDays: int = 30
    # Chuỗi net theo phiên để vẽ trend (10-20 phiên gần nhất).
    # Mặc định = 20.
    trendSessions: int = 20


def _ssi_env_consumer_credentials() -> tuple[Optional[str], Optional[str]]:
    consumer_id = os.environ.get("SSI_FC_CONSUMER_ID") or os.environ.get("SSI_FC_CONSUMERID")
    consumer_secret = (
        os.environ.get("SSI_FC_CONSUMER_SECRET")
        or os.environ.get("SSI_FC_CONSUMERSECRET")
        or os.environ.get("SSI_FC_CONSUMERSECRECT")
    )
    return consumer_id, consumer_secret


def _parse_sci_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        return x if x == x and abs(x) < 1e30 else None
    try:
        s = str(v).strip()
        if not s:
            return None
        s = s.replace(",", "")
        x = float(s)
        return x if x == x and abs(x) < 1e30 else None
    except Exception:
        return None


def _parse_fc_trading_date(date_str: str) -> Optional[datetime.date]:
    import datetime
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y"):
        try:
            return datetime.datetime.strptime(date_str, fmt).date()
        except Exception:
            continue
    return None


def _format_iso_date(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def _ssi_get_access_token(
    base_url: str,
    consumer_id: Optional[str],
    consumer_secret: Optional[str],
    timeout: int = 20,
) -> Optional[str]:
    if not consumer_id or not consumer_secret:
        return None

    import urllib.request

    token_url = f"{base_url.rstrip('/')}/api/v2/Market/AccessToken"
    payload = {"consumerID": consumer_id, "consumerSecret": consumer_secret}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    req = urllib.request.Request(
        token_url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
        data = json.loads(raw)

    status = data.get("status", data.get("Status", None))
    # Some responses may use string statuses; accept both.
    if status not in (200, "SUCCESS", "Success", None):
        # If status exists but isn't success, still fall through to try to extract token.
        pass

    token_data = data.get("data") or data.get("Data") or {}
    token = token_data.get("accessToken") or token_data.get("access_token") or data.get("accessToken")
    return str(token) if token else None


def _ssi_get_daily_stock_price(
    base_url: str,
    token: str,
    symbol: str,
    from_date: datetime.date,
    to_date: datetime.date,
    page_index: int = 1,
    page_size: int = 200,
    market: str = "",
    timeout: int = 25,
) -> List[Dict[str, Any]]:
    if not token:
        return []

    import urllib.parse
    import urllib.request

    endpoint = f"{base_url.rstrip('/')}/api/v2/Market/DailyStockPrice"
    from_str = from_date.strftime("%d/%m/%Y")
    to_str = to_date.strftime("%d/%m/%Y")

    params = {
        "symbol": symbol,
        "fromDate": from_str,
        "toDate": to_str,
        "pageIndex": page_index,
        "pageSize": page_size,
        "market": market,
    }
    qs = urllib.parse.urlencode(params)
    url = f"{endpoint}?{qs}"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
        data = json.loads(raw)

    items = data.get("data") or data.get("Data") or []
    if not isinstance(items, list):
        return []
    return items


def _compute_money_flow_for_symbol(
    symbol: str,
    total_days: int,
    trend_sessions: int,
    ssi_base_url: str,
    token: Optional[str],
) -> Optional[Dict[str, Any]]:
    import datetime

    if not token:
        return None

    today = datetime.date.today()
    start_trend = today - datetime.timedelta(days=max(60, trend_sessions * 2))
    start_total = today - datetime.timedelta(days=max(1, total_days))

    items = _ssi_get_daily_stock_price(
        base_url=ssi_base_url,
        token=token,
        symbol=symbol,
        from_date=start_trend,
        to_date=today,
        page_index=1,
        page_size=500,
        market="",
    )
    if not items:
        return None

    parsed: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        trading_date_str = it.get("TradingDate") or it.get("tradingDate") or it.get("Trading_date")
        if not trading_date_str:
            continue
        d = _parse_fc_trading_date(str(trading_date_str))
        if not d:
            continue

        foreign_buy = _parse_sci_float(it.get("ForeignBuyValTotal"))
        foreign_sell = _parse_sci_float(it.get("ForeignSellValTotal"))
        total_buy = _parse_sci_float(it.get("TotalBuyTrade"))
        total_sell = _parse_sci_float(it.get("TotalSellTrade"))
        if foreign_buy is None or foreign_sell is None:
            continue

        # Tự doanh (proprietary) = tổng giao dịch (total) - phần của khối ngoại.
        if total_buy is None:
            total_buy = foreign_buy
        if total_sell is None:
            total_sell = foreign_sell

        proprietary_buy = max(0.0, total_buy - foreign_buy)
        proprietary_sell = max(0.0, total_sell - foreign_sell)

        parsed.append(
            {
                "date": _format_iso_date(d),
                "foreignBuy": foreign_buy,
                "foreignSell": foreign_sell,
                "proprietaryBuy": proprietary_buy,
                "proprietarySell": proprietary_sell,
                "foreignNet": foreign_buy - foreign_sell,
                "proprietaryNet": proprietary_buy - proprietary_sell,
                "d": d,
            }
        )

    if not parsed:
        return None

    parsed.sort(key=lambda x: x["d"])
    trend_tail = parsed[-max(1, trend_sessions) :]
    if not trend_tail:
        return None

    trend_foreign = [
        {
            "date": p["date"],
            "buy": round(p["foreignBuy"]),
            "sell": round(p["foreignSell"]),
            "net": round(p["foreignNet"]),
        }
        for p in trend_tail
    ]
    trend_prop = [
        {
            "date": p["date"],
            "buy": round(p["proprietaryBuy"]),
            "sell": round(p["proprietarySell"]),
            "net": round(p["proprietaryNet"]),
        }
        for p in trend_tail
    ]

    # Totals: sum foreign/proprietary in last `total_days` calendar window.
    totals = [p for p in parsed if p["d"] >= start_total]
    foreignBuy = round(sum(p["foreignBuy"] for p in totals))
    foreignSell = round(sum(p["foreignSell"] for p in totals))
    proprietaryBuy = round(sum(p["proprietaryBuy"] for p in totals))
    proprietarySell = round(sum(p["proprietarySell"] for p in totals))

    return {
        "foreignBuy": foreignBuy,
        "foreignSell": foreignSell,
        "proprietaryBuy": proprietaryBuy,
        "proprietarySell": proprietarySell,
        "foreignNetSeries": trend_foreign,
        "proprietaryNetSeries": trend_prop,
    }


@app.post("/api/moneyflow")
def api_moneyflow(req: MoneyFlowRequest):
    """
    POST /api/moneyflow
    Trả về:
      - Tổng foreign/proprietary trong cửa sổ `totalDays` (mặc định 30 ngày lịch)
      - Chuỗi net theo từng phiên `trendSessions` (mặc định 20 phiên gần nhất)
    """
    consumer_id, consumer_secret = _ssi_env_consumer_credentials()
    ssi_base_url = os.environ.get("SSI_FC_BASE_URL", "https://fc-data.ssi.com.vn")

    token = _ssi_get_access_token(
        base_url=ssi_base_url,
        consumer_id=consumer_id,
        consumer_secret=consumer_secret,
    )
    if not token:
        return JSONResponse(content={"data": {}}, status_code=200)

    tickers = req.tickers or []
    total_days = int(req.totalDays or 30)
    trend_sessions = int(req.trendSessions or 20)
    unique = list({str(t).strip().upper() for t in tickers if t})

    # Bật song song để giảm tổng thời gian khi có nhiều mã.
    workers = min(8, max(1, len(unique)))
    out: Dict[str, Any] = {}
    if unique:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for symbol in unique:
                cache_key = f"{symbol}|{total_days}|{trend_sessions}"
                cached = _cache_get(_moneyflow_cache, cache_key)
                if cached is not None:
                    out[symbol] = cached
                    continue
                futures[
                    pool.submit(
                        _compute_money_flow_for_symbol,
                        symbol,
                        total_days,
                        trend_sessions,
                        ssi_base_url,
                        token,
                    )
                ] = (symbol, cache_key)
            for fut, symbol in futures.items():
                try:
                    res = fut.result(timeout=90)
                    if res:
                        out[symbol[0]] = res
                        _cache_set(
                            _moneyflow_cache,
                            symbol[1],
                            res,
                            _MONEYFLOW_CACHE_TTL_SECONDS,
                        )
                except Exception:
                    continue

    return JSONResponse(content={"data": out})


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
        cache_key = f"{symbol}|{','.join(SOURCES)}|heavy:{int(_ENABLE_HEAVY_FIELDS)}"
        cached = _cache_get(_fundamentals_cache, cache_key)
        if cached is not None:
            data[symbol] = cached
            continue
        try:
            item = _extract_for_sources(symbol, SOURCES)
            if any(x is not None for x in item.values()):
                # Loại bỏ None để tránh ResponseValidationError
                cleaned = {k: v for k, v in item.items() if v is not None}
                data[symbol] = cleaned
                _cache_set(
                    _fundamentals_cache,
                    cache_key,
                    cleaned,
                    _FUNDAMENTALS_CACHE_TTL_SECONDS,
                )
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

        # Vercel handler: phân nhánh theo path để hỗ trợ nhiều API.
        if "moneyflow" in str(getattr(self, "path", "")).lower():
            try:
                total_days = int(payload.get("totalDays") or 30)
                trend_sessions = int(payload.get("trendSessions") or 20)
            except Exception:
                total_days = 30
                trend_sessions = 20

            consumer_id, consumer_secret = _ssi_env_consumer_credentials()
            ssi_base_url = os.environ.get("SSI_FC_BASE_URL", "https://fc-data.ssi.com.vn")
            token = _ssi_get_access_token(
                base_url=ssi_base_url,
                consumer_id=consumer_id,
                consumer_secret=consumer_secret,
            )
            if not token:
                out = {"data": {}}
                out_json = json.dumps(out, ensure_ascii=False)
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(out_json.encode("utf-8"))
                return

            out: Dict[str, Any] = {}
            if unique:
                workers = min(8, max(1, len(unique)))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = {
                        pool.submit(
                            _compute_money_flow_for_symbol,
                            symbol,
                            total_days,
                            trend_sessions,
                            ssi_base_url,
                            token,
                        ): symbol
                        for symbol in unique
                    }
                    for fut, symbol in futures.items():
                        try:
                            res = fut.result(timeout=90)
                            if res:
                                out[symbol] = res
                        except Exception:
                            continue

            out_json = json.dumps({"data": out}, ensure_ascii=False)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(out_json.encode("utf-8"))
            return

        data: Dict[str, Dict[str, Optional[float]]] = {}
        for symbol in unique:
            try:
                item = _extract_for_sources(symbol, SOURCES)
                if any(x is not None for x in item.values()):
                    data[symbol] = item
            except Exception:
                continue

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
