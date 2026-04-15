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
from collections import deque
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


class MoneyFlowRequest(BaseModel):
    tickers: List[str] = []
    # Tổng hợp KN/TD theo số ngày lịch gần nhất (mặc định 30).
    totalDays: int = 30
    # Chuỗi net theo phiên (mặc định 20 phiên gần nhất).
    trendSessions: int = 20
    # Alias cũ (một số client gửi `days` thay cho totalDays).
    days: Optional[int] = None


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
_VNINDEX_CACHE_TTL_SECONDS = max(
    10, int(os.environ.get("VNINDEX_CACHE_TTL_SECONDS", "300"))
)
_VN30_BREADTH_CACHE_TTL_SECONDS = max(
    10, int(os.environ.get("VN30_BREADTH_CACHE_TTL_SECONDS", "600"))
)
_cache_lock = Lock()
_fundamentals_cache: Dict[str, Dict[str, Any]] = {}
_moneyflow_cache: Dict[str, Dict[str, Any]] = {}
_vnindex_cache: Dict[str, Dict[str, Any]] = {}
_vn30_breadth_cache: Dict[str, Dict[str, Any]] = {}

# Rate limiter for vnstock API calls
class VnstockRateLimiter:
    """Simple rate limiter to prevent exceeding vnstock API limits."""
    
    def __init__(self, max_calls: int = 10, time_window: int = 60):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls: deque = deque()
        self.lock = Lock()
        self._rate_limited_until = 0.0
    
    def set_rate_limited(self, seconds: int = 60):
        """Mark as rate limited for a duration."""
        with self.lock:
            self._rate_limited_until = time.time() + seconds
    
    def is_rate_limited(self) -> bool:
        """Check if currently in rate-limited cooldown."""
        with self.lock:
            return time.time() < self._rate_limited_until
    
    def can_proceed(self) -> tuple:
        """Check if a call can proceed. Returns (can_proceed, wait_time)."""
        if self.is_rate_limited():
            wait_time = self._rate_limited_until - time.time()
            return False, max(0, wait_time)
        
        with self.lock:
            now = time.time()
            # Remove calls outside the time window
            while self.calls and self.calls[0] < now - self.time_window:
                self.calls.popleft()
            
            if len(self.calls) < self.max_calls:
                return True, 0.0
            
            # Calculate wait time
            wait_time = self.calls[0] + self.time_window - now
            return False, max(0, wait_time)
    
    def record_call(self):
        """Record a successful API call."""
        with self.lock:
            self.calls.append(time.time())
    
    def wait_if_needed(self, max_wait: float = 5.0) -> bool:
        """Wait if needed. Returns False if wait > max_wait."""
        can_proceed, wait_time = self.can_proceed()
        if can_proceed:
            return True
        if wait_time > max_wait:
            return False
        time.sleep(wait_time + 0.1)
        return True

# Initialize rate limiter with conservative limit (50% of Guest tier for safety)
_vnstock_limiter = VnstockRateLimiter(
    max_calls=int(os.environ.get("VNSTOCK_MAX_CALLS_PER_MINUTE", "10")),
    time_window=60
)


def _cache_get(cache: Dict[str, Dict[str, Any]], key: str, include_expired: bool = False) -> Optional[Any]:
    """Get cached value. If include_expired=True, return even expired values (for fallback)."""
    now = time.time()
    with _cache_lock:
        entry = cache.get(key)
        if not entry:
            return None
        if entry.get("expires_at", 0) <= now:
            if include_expired:
                return entry.get("value")
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


def _df_sum_columns(df: Any, names: tuple) -> Optional[float]:
    """Cộng các cột có trong DataFrame; trả None nếu không có cột nào khớp."""
    if df is None or not hasattr(df, "columns"):
        return None
    cols = getattr(df, "columns", None)
    if cols is None:
        return None
    acc = 0.0
    hit = False
    for n in names:
        if n in cols:
            try:
                acc += float(df[n].sum())
                hit = True
            except Exception:
                pass
    return acc if hit else None


def _infer_foreign_buy_sell(fr_df: Any) -> tuple[Optional[float], Optional[float]]:
    """Chuẩn hoá nhiều kiểu đặt tên cột từ VCI/TCBS/vnstock_data."""
    if fr_df is None or (hasattr(fr_df, "empty") and fr_df.empty):
        return None, None
    if isinstance(fr_df, dict):
        fb = _safe_float(
            fr_df.get("fr_buy_value")
            or fr_df.get("foreign_buy_value")
            or fr_df.get("buy_value")
        )
        fs = _safe_float(
            fr_df.get("fr_sell_value")
            or fr_df.get("foreign_sell_value")
            or fr_df.get("sell_value")
        )
        return fb, fs
    if not hasattr(fr_df, "columns"):
        return None, None
    fb = _df_sum_columns(fr_df, ("fr_buy_value", "foreign_buy_value", "buy_value"))
    if fb is None:
        fb = _df_sum_columns(fr_df, ("fr_buy_value_matched", "fr_buy_value_deal"))
    fs = _df_sum_columns(fr_df, ("fr_sell_value", "foreign_sell_value", "sell_value"))
    if fs is None:
        fs = _df_sum_columns(fr_df, ("fr_sell_value_matched", "fr_sell_value_deal"))
    return _safe_float(fb), _safe_float(fs)


def _extract_moneyflow_from_trading(trading: Any, start_str: str, end_str: str) -> Dict[str, Optional[float]]:
    """Đọc một instance Trading (vnstock_data) đã khởi tạo."""
    out: Dict[str, Optional[float]] = {
        "foreignBuy": None,
        "foreignSell": None,
        "proprietaryBuy": None,
        "proprietarySell": None,
        "foreignRoomCurrent": None,
        "foreignRoomTotal": None,
        "foreignOwnership": None,
    }
    fr_df = None
    try:
        fr_df = trading.foreign_trade(start=start_str, end=end_str)
    except Exception:
        try:
            fr_df = trading.foreign_trading(start=start_str, end=end_str)
        except Exception:
            fr_df = None

    if fr_df is not None and not (hasattr(fr_df, "empty") and fr_df.empty):
        fb, fs = _infer_foreign_buy_sell(fr_df)
        out["foreignBuy"], out["foreignSell"] = fb, fs
        if hasattr(fr_df, "columns"):
            if "fr_current_room" in fr_df.columns:
                out["foreignRoomCurrent"] = _safe_float(fr_df["fr_current_room"].iloc[-1])
            if "fr_total_room" in fr_df.columns:
                out["foreignRoomTotal"] = _safe_float(fr_df["fr_total_room"].iloc[-1])
            if "fr_remaining_room" in fr_df.columns:
                out["foreignRoomCurrent"] = out["foreignRoomCurrent"] or _safe_float(
                    fr_df["fr_remaining_room"].iloc[-1]
                )
            if "fr_ownership" in fr_df.columns:
                out["foreignOwnership"] = _safe_float(fr_df["fr_ownership"].iloc[-1])
        elif isinstance(fr_df, dict):
            out["foreignRoomCurrent"] = _safe_float(fr_df.get("fr_current_room") or fr_df.get("fr_remaining_room"))
            out["foreignRoomTotal"] = _safe_float(fr_df.get("fr_total_room"))
            out["foreignOwnership"] = _safe_float(fr_df.get("fr_ownership"))

    prop_df = None
    try:
        prop_df = trading.prop_trade(start=start_str, end=end_str, resolution="1D")
    except Exception:
        try:
            prop_df = trading.proprietary_trade(start=start_str, end=end_str)
        except Exception:
            prop_df = None

    buy_candidates = (
        "total_buy_trade_value",
        "total_deal_buy_trade_value",
        "buy_value",
        "prop_buy_value",
    )
    sell_candidates = (
        "total_sell_trade_value",
        "total_deal_sell_trade_value",
        "sell_value",
        "prop_sell_value",
    )

    if prop_df is not None and not (hasattr(prop_df, "empty") and prop_df.empty):
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


def _moneyflow_dict_has_values(m: Dict[str, Optional[float]]) -> bool:
    for k in ("foreignBuy", "foreignSell", "proprietaryBuy", "proprietarySell"):
        v = m.get(k)
        if isinstance(v, (int, float)) and v == v and abs(float(v)) > 1e-9:
            return True
    return False


def _get_moneyflow(symbol: str, days: int = 30) -> Optional[Dict[str, Optional[float]]]:
    """
    Lấy khối ngoại/tự doanh (mua/bán) trên một cửa sổ ngày gần nhất.

    Response fields:
      - foreignBuy, foreignSell (giá trị mua/bán theo NĐTNN)
      - proprietaryBuy, proprietarySell (giá trị mua/bán tự doanh)
    """
    empty: Dict[str, Optional[float]] = {
        "foreignBuy": None,
        "foreignSell": None,
        "proprietaryBuy": None,
        "proprietarySell": None,
        "foreignRoomCurrent": None,
        "foreignRoomTotal": None,
        "foreignOwnership": None,
    }
    if _Trading is None:
        return empty

    try:
        from datetime import date, timedelta

        end_d = date.today()
        start_d = end_d - timedelta(days=days)
        start_str = start_d.strftime("%Y-%m-%d")
        end_str = end_d.strftime("%Y-%m-%d")

        merged = dict(empty)
        for src in ("vci", "tcbs", "ssi"):
            try:
                trading = _Trading(symbol=symbol, source=src)
            except Exception:
                continue
            part = _extract_moneyflow_from_trading(trading, start_str, end_str)
            for k, v in part.items():
                if v is not None and (merged.get(k) is None):
                    merged[k] = v
            if _moneyflow_dict_has_values(merged):
                break

        return merged
    except Exception:
        return empty


def _vnstock_moneyflow_to_api_shape(
    vn: Dict[str, Optional[float]], total_days: int
) -> Optional[Dict[str, Any]]:
    """Chuyển dict phẳng từ _get_moneyflow sang payload giống SSI (không có series)."""
    if not vn:
        return None
    fb = int(round(float(vn.get("foreignBuy") or 0)))
    fs = int(round(float(vn.get("foreignSell") or 0)))
    pb = int(round(float(vn.get("proprietaryBuy") or 0)))
    ps = int(round(float(vn.get("proprietarySell") or 0)))
    if fb == 0 and fs == 0 and pb == 0 and ps == 0:
        return None
    return {
        "foreignBuy": fb,
        "foreignSell": fs,
        "proprietaryBuy": pb,
        "proprietarySell": ps,
        "foreignNetSeries": None,
        "proprietaryNetSeries": None,
    }


def _moneyflow_api_totals_nonzero(m: Optional[Dict[str, Any]]) -> bool:
    if not m or not isinstance(m, dict):
        return False
    for k in ("foreignBuy", "foreignSell", "proprietaryBuy", "proprietarySell"):
        v = m.get(k)
        if isinstance(v, (int, float)) and v == v and abs(float(v)) > 1e-6:
            return True
    return False


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


def _vnstock_safe_quote_history(quote: Any, **kwargs: Any) -> Any:
    """
    Gọi quote.history; thư viện vnai có thể sys.exit khi hết quota — không nằm trong nhánh Exception.
    """
    try:
        return quote.history(**kwargs)
    except SystemExit:
        return None


def _yahoo_try_symbol_vnindex_bars(
    sym: str, days: int, rng: str
) -> Optional[List[Dict[str, float]]]:
    """Một lần gọi chart Yahoo cho một symbol + range."""
    import urllib.parse
    import urllib.request

    try:
        enc = urllib.parse.quote(sym, safe="")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{enc}?interval=1d&range={rng}"
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (compatible; Fundamentals/1.0)"}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        results = data.get("chart", {}).get("result") or []
        if not results:
            return None
        quote_block = results[0].get("indicators", {}).get("quote") or [{}]
        q0 = quote_block[0] if quote_block else {}
        closes_raw = q0.get("close") or []
        vols_raw = q0.get("volume") or []
        if not closes_raw:
            return None
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
        if len(bars_y) < 20:
            return None
        return bars_y[-days:] if len(bars_y) > days else bars_y
    except Exception:
        return None


def _yahoo_fetch_vnindex_bars(days: int) -> Optional[List[Dict[str, float]]]:
    """
    Yahoo Finance — thử ^VNINDEX và VNINDEX.VN (range 2y/1y/5y).
    Hai mã đôi khi cho đóng cửa khác nhau; chọn chuỗi có last trong dải VN-Index
    hợp lý và ưu tiên last cao hơn (tránh nhầm series ~1260 khi spot ~1800).
    """
    if days <= 0:
        days = 260
    if days > 400:
        ranges = ("5y",)
    elif days > 220:
        ranges = ("2y", "1y", "5y")
    else:
        ranges = ("1y", "2y", "5y")
    candidates: List[List[Dict[str, float]]] = []
    for sym in ("^VNINDEX", "VNINDEX.VN"):
        for rng in ranges:
            bars = _yahoo_try_symbol_vnindex_bars(sym, days, rng)
            if bars:
                candidates.append(bars)
    if not candidates:
        return None

    def _last(bs: List[Dict[str, float]]) -> float:
        return float(bs[-1]["close"])

    def _max_c(bs: List[Dict[str, float]]) -> float:
        return max(float(x["close"]) for x in bs)

    lo = float(os.environ.get("VNINDEX_YAHOO_PLAUSIBLE_MIN", "900"))
    hi = float(os.environ.get("VNINDEX_YAHOO_PLAUSIBLE_MAX", "3200"))
    plausible = [b for b in candidates if lo <= _last(b) <= hi]
    pool = plausible if plausible else candidates
    # Ưu tiên chuỗi có đỉnh lịch sử cao (series ~1260 max ~1.3k; ~1800 max ~1.9k+).
    pool.sort(key=lambda bs: (_max_c(bs), _last(bs)), reverse=True)
    return pool[0]


def _get_vnindex_bars(days: int = 260) -> Optional[List[Dict[str, float]]]:
    """Chuỗi nến daily VN-Index (cũ → mới): close, volume (0 nếu nguồn không có)."""
    if days <= 0:
        days = 260
    
    # Try cache first
    cache_key = f"vnindex_bars_{days}"
    cached = _cache_get(_vnindex_cache, cache_key)
    if cached is not None:
        return cached

    stale_any = _cache_get(_vnindex_cache, cache_key, include_expired=True)

    # Đang cooldown vnstock: vẫn thử Yahoo / SSI (không đụng quota vnai).
    if _vnstock_limiter.is_rate_limited():
        bars_y = _yahoo_fetch_vnindex_bars(days)
        if bars_y:
            _cache_set(_vnindex_cache, cache_key, bars_y, _VNINDEX_CACHE_TTL_SECONDS)
            return bars_y
        bars_s = _ssi_vnindex_bars_from_fastconnect(days)
        if bars_s:
            _cache_set(_vnindex_cache, cache_key, bars_s, _VNINDEX_CACHE_TTL_SECONDS)
            return bars_s
        return stale_any

    skip_vn = str(os.environ.get("VNINDEX_BARS_SKIP_VNSTOCK", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

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

    def _on_vnstock_quota_exit() -> None:
        _vnstock_limiter.set_rate_limited(90)

    # 1) Yahoo — ưu tiên: ổn định trên Render, không quota vnstock/vnai
    bars_yahoo = _yahoo_fetch_vnindex_bars(days)
    if bars_yahoo:
        _cache_set(_vnindex_cache, cache_key, bars_yahoo, _VNINDEX_CACHE_TTL_SECONDS)
        return bars_yahoo

    # 2) SSI FastConnect DailyStockPrice (symbol VNINDEX) — nếu có SSI_FC_* env
    bars_fc = _ssi_vnindex_bars_from_fastconnect(days)
    if bars_fc:
        _cache_set(_vnindex_cache, cache_key, bars_fc, _VNINDEX_CACHE_TTL_SECONDS)
        return bars_fc

    # 3) Robotstock
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
                out_rs = bars_rs[-days:] if len(bars_rs) > days else bars_rs
                _cache_set(_vnindex_cache, cache_key, out_rs, _VNINDEX_CACHE_TTL_SECONDS)
                return out_rs
    except Exception:
        pass

    # 4) vnstock — cuối (tốn quota vnai; có thể sys.exit). Tắt: VNINDEX_BARS_SKIP_VNSTOCK=1
    if skip_vn:
        return stale_any

    if Quote is not None:
        for source in ("KBS", "TCBS", "DNSE"):
            if not _vnstock_limiter.wait_if_needed(max_wait=3.0):
                break
            try:
                quote = Quote(symbol="VNINDEX", source=source)
                df = _vnstock_safe_quote_history(quote, length="1Y", interval="1D")
                if df is None:
                    _on_vnstock_quota_exit()
                    if stale_any:
                        return stale_any
                    break
                _vnstock_limiter.record_call()
                bars = _bars_from_df(df)
                if bars:
                    _cache_set(_vnindex_cache, cache_key, bars, _VNINDEX_CACHE_TTL_SECONDS)
                    return bars
            except Exception as e:
                error_msg = str(e).lower()
                if "rate limit" in error_msg or "too many" in error_msg:
                    _vnstock_limiter.set_rate_limited(60)
                    if stale_any:
                        return stale_any
                    break
                continue
        try:
            from datetime import date, timedelta

            end_d = date.today()
            start_d = end_d - timedelta(days=days * 2)
            if _vnstock_limiter.wait_if_needed(max_wait=3.0):
                quote = Quote(symbol="VNINDEX", source="KBS")
                df = _vnstock_safe_quote_history(
                    quote,
                    start=start_d.strftime("%Y-%m-%d"),
                    end=end_d.strftime("%Y-%m-%d"),
                    interval="1D",
                )
                if df is None:
                    _on_vnstock_quota_exit()
                else:
                    _vnstock_limiter.record_call()
                    bars = _bars_from_df(df)
                    if bars:
                        _cache_set(_vnindex_cache, cache_key, bars, _VNINDEX_CACHE_TTL_SECONDS)
                        return bars
        except Exception:
            pass

    if get_index_series is not None and _vnstock_limiter.wait_if_needed(max_wait=3.0):
        try:
            try:
                df = get_index_series(index_code="VNINDEX", time_range="OneYear")
            except SystemExit:
                df = None
                _on_vnstock_quota_exit()
            if df is not None:
                _vnstock_limiter.record_call()
                bars = _bars_from_df(df)
                if bars:
                    _cache_set(_vnindex_cache, cache_key, bars, _VNINDEX_CACHE_TTL_SECONDS)
                    return bars
        except Exception:
            pass

    if stock_historical_data is not None:
        try:
            from datetime import date, timedelta

            end_d = date.today()
            start_d = end_d - timedelta(days=days * 2)
            start_str = start_d.strftime("%Y-%m-%d")
            end_str = end_d.strftime("%Y-%m-%d")
            if _vnstock_limiter.wait_if_needed(max_wait=3.0):
                for kwargs in [
                    {"symbol": "VNINDEX", "start_date": start_str, "end_date": end_str, "type": "index"},
                    {"symbol": "VNINDEX", "start_date": start_str, "end_date": end_str},
                ]:
                    try:
                        try:
                            df = stock_historical_data(**kwargs)
                        except SystemExit:
                            df = None
                            _on_vnstock_quota_exit()
                        if df is None:
                            continue
                        _vnstock_limiter.record_call()
                        bars = _bars_from_df(df)
                        if bars:
                            _cache_set(_vnindex_cache, cache_key, bars, _VNINDEX_CACHE_TTL_SECONDS)
                            return bars
                    except TypeError:
                        try:
                            try:
                                df = stock_historical_data("VNINDEX", start_str, end_str)
                            except SystemExit:
                                df = None
                                _on_vnstock_quota_exit()
                            if df is None:
                                continue
                            _vnstock_limiter.record_call()
                            bars = _bars_from_df(df)
                            if bars:
                                _cache_set(_vnindex_cache, cache_key, bars, _VNINDEX_CACHE_TTL_SECONDS)
                                return bars
                        except Exception:
                            pass
        except Exception:
            pass

    return stale_any


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


def _should_replace_vnindex_levels_with_yahoo(
    backend_last: float, yahoo_last: float
) -> bool:
    """Giống logic reconcile trên Next: backend ~1260 vs Yahoo ~1800."""
    try:
        b = float(backend_last)
        y = float(yahoo_last)
    except (TypeError, ValueError):
        return False
    if not (900 <= y <= 3400):
        return False
    if b < 1350 and y > 1550:
        return True
    if y > b + max(40.0, 0.025 * b):
        return True
    if b > 3200 and y < b - 50.0:
        return True
    return False


def _yahoo_vnindex_reference_last() -> Optional[float]:
    """Đóng cửa gần nhất từ Yahoo (5d), max giữa ^VNINDEX và VNINDEX.VN trong dải hợp lý."""
    best: Optional[float] = None
    for sym in ("^VNINDEX", "VNINDEX.VN"):
        bars = _yahoo_try_symbol_vnindex_bars(sym, 10, "5d")
        if not bars:
            continue
        v = float(bars[-1]["close"])
        if 900 <= v <= 3400 and (best is None or v > best):
            best = v
    return best


def _yahoo_vnindex_volume_tail(n: int) -> Optional[List[float]]:
    """Lấy n volume daily gần nhất từ Yahoo (cũ → mới), căn chỉnh với đuôi chuỗi giá."""
    import urllib.request

    n = max(1, int(n))
    for yahoo_symbol in ("%5EVNINDEX", "VNINDEX.VN"):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?interval=1d&range=2y"
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
            vols: List[float] = []
            for i, c in enumerate(closes_raw):
                if c is None or not isinstance(c, (int, float)):
                    continue
                vv = (
                    float(vols_raw[i])
                    if i < len(vols_raw) and isinstance(vols_raw[i], (int, float))
                    else 0.0
                )
                vols.append(vv)
            if len(vols) >= n:
                return vols[-n:]
        except Exception:
            continue
    return None


def _merge_volumes_if_all_zero(bars: List[Dict[str, float]]) -> List[Dict[str, float]]:
    """
    Nhiều nguồn vnstock trả VNINDEX không có volume (toàn 0) → thanh khoản N/A.
    Ghép volume theo cùng độ dài từ Yahoo (cùng thứ tự cuối chuỗi).
    """
    if not bars:
        return bars
    total_v = sum(float(b.get("volume") or 0.0) for b in bars)
    if total_v > 1e-3:
        return bars
    ytail = _yahoo_vnindex_volume_tail(len(bars))
    if not ytail or len(ytail) != len(bars):
        return bars
    return [{"close": float(b["close"]), "volume": float(ytail[i])} for i, b in enumerate(bars)]


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
    """Check if a VN30 stock is above MA200 with caching."""
    # Check cache first (cache each symbol for 30 minutes since MA200 changes slowly)
    cache_key = f"vn30_ma200_{sym}"
    cached = _cache_get(_vn30_breadth_cache, cache_key)
    if cached is not None:
        return cached
    
    # Check rate limiter
    if not _vnstock_limiter.wait_if_needed(max_wait=2.0):
        return None  # Skip if rate limited
    
    closes = _get_equity_close_prices(sym, 220)
    _vnstock_limiter.record_call()
    
    if not closes or len(closes) < 200:
        return None
    last_c = closes[-1]
    ma200 = sum(closes[-200:]) / 200.0
    result = last_c > ma200
    
    # Cache for 30 minutes (MA200 data changes slowly)
    _cache_set(_vn30_breadth_cache, cache_key, result, 1800)
    
    return result


def _compute_vn30_above_ma200_breadth() -> Dict[str, Any]:
    """
    Compute VN30 breadth with aggressive caching.
    This is cached for 10 minutes to avoid 30+ API calls per request.
    """
    cache_key = "vn30_breadth_full"
    
    # Try cache first
    cached = _cache_get(_vn30_breadth_cache, cache_key)
    if cached is not None:
        return cached
    
    # Check if rate limited - return stale data if available
    if _vnstock_limiter.is_rate_limited():
        stale = _cache_get(_vn30_breadth_cache, cache_key, include_expired=True)
        if stale:
            return stale
        # Return empty result if no stale data
        return {
            "vn30AboveMa200Pct": None,
            "vn30AboveMa200Count": None,
            "vn30BreadthSampleSize": None,
            "vn30BreadthFailedFetch": None,
        }
    
    symbols = _vn30_symbol_list()
    above = 0
    total = 0
    failed = 0
    
    # Reduce workers to avoid burst requests (was 10, now 2-3)
    workers = min(2, max(1, len(symbols) // 15))
    
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
    
    result = {
        "vn30AboveMa200Pct": pct,
        "vn30AboveMa200Count": above,
        "vn30BreadthSampleSize": total,
        "vn30BreadthFailedFetch": failed,
    }
    
    # Cache for 10 minutes (breadth doesn't change frequently)
    _cache_set(_vn30_breadth_cache, cache_key, result, _VN30_BREADTH_CACHE_TTL_SECONDS)
    
    return result


def _volume_today_vs_avg20(bars: List[Dict[str, float]]) -> Dict[str, Any]:
    vols = [b.get("volume") or 0.0 for b in bars]
    if len(vols) < 2:
        return {
            "volume_today": None,
            "volume_avg20": None,
            "volume_vs_avg20_pct": None,
        }
    today_v = vols[-1]
    prev_slice = vols[-21:-1] if len(vols) >= 21 else vols[:-1]
    avg20 = (
        sum(prev_slice) / len(prev_slice) if prev_slice else None
    )
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
    # Try cache first (5 minutes)
    cache_key = "vnindex_overview_full"
    cached = _cache_get(_vnindex_cache, cache_key)
    if cached is not None:
        return cached
    
    bars = _get_vnindex_bars(260)
    if not bars or len(bars) < 20:
        # Try to return stale cache if rate limited
        stale = _cache_get(_vnindex_cache, cache_key, include_expired=True)
        return stale

    bars = _merge_volumes_if_all_zero(bars)
    bars = _normalize_vnindex_bars(bars)
    prices = [b["close"] for b in bars]
    last = prices[-1]
    if last < 100 or last > 5000:
        return None

    # Robotstock / normalize đôi khi ~1260 trong khi Yahoo ~1800 — thay chuỗi nến Yahoo đầy đủ.
    y_ref = _yahoo_vnindex_reference_last()
    if y_ref is not None and _should_replace_vnindex_levels_with_yahoo(float(last), y_ref):
        alt = _yahoo_fetch_vnindex_bars(260)
        if alt and len(alt) >= 20:
            bars = _merge_volumes_if_all_zero(alt)
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
    
    # Cache the complete overview for 5 minutes
    _cache_set(_vnindex_cache, cache_key, out, _VNINDEX_CACHE_TTL_SECONDS)
    
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
        "rate_limiter": {
            "calls_in_last_minute": len(_vnstock_limiter.calls),
            "is_rate_limited": _vnstock_limiter.is_rate_limited(),
            "max_calls_per_minute": _vnstock_limiter.max_calls,
        },
        "cache_stats": {
            "vnindex_entries": len(_vnindex_cache),
            "vn30_breadth_entries": len(_vn30_breadth_cache),
            "fundamentals_entries": len(_fundamentals_cache),
            "moneyflow_entries": len(_moneyflow_cache),
        }
    })


@app.get("/api/vnindex-overview")
@app.get("/vnindex-overview")
def api_vnindex_overview():
    """
    GET VN-Index overview: last, MA(20/50/200), RSI14, pha thị trường (nhãn + streak MA200),
    thanh khoản (20 phiên vs phiên hiện tại), % mẫu VN30 trên MA200.
    With circuit breaker for rate limits.
    """
    try:
        result = _compute_vnindex_overview()
        if result is None:
            # Check if we have stale cache
            cache_key = "vnindex_overview_full"
            stale = _cache_get(_vnindex_cache, cache_key, include_expired=True)
            if stale:
                return JSONResponse(content={
                    **stale,
                    "_cached": True,
                    "_warning": "Using cached data due to rate limits or service issues"
                })
            
            return JSONResponse(
                content={"error": "Không lấy được dữ liệu VN-Index. Vui lòng thử lại sau."},
                status_code=503
            )
        return JSONResponse(content=result)
    except Exception as e:
        error_msg = str(e).lower()
        if "rate limit" in error_msg or "too many" in error_msg:
            _vnstock_limiter.set_rate_limited(60)
            # Try to return stale cache
            cache_key = "vnindex_overview_full"
            stale = _cache_get(_vnindex_cache, cache_key, include_expired=True)
            if stale:
                return JSONResponse(content={
                    **stale,
                    "_cached": True,
                    "_warning": "Rate limited, using cached data"
                })
        
        return JSONResponse(
            content={"error": "Không lấy được dữ liệu VN-Index"},
            status_code=503
        )


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


def _ssi_collect_daily_stock_price_paged(
    base_url: str,
    token: str,
    symbol: str,
    from_date: Any,
    to_date: Any,
    page_size: int = 500,
    max_pages: int = 12,
) -> List[Dict[str, Any]]:
    """Gom nhiều trang DailyStockPrice (SSI trả tối đa pageSize bản ghi/trang)."""
    if not token:
        return []
    all_rows: List[Dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        chunk = _ssi_get_daily_stock_price(
            base_url=base_url,
            token=token,
            symbol=symbol,
            from_date=from_date,
            to_date=to_date,
            page_index=page,
            page_size=page_size,
            market="",
        )
        if not chunk:
            break
        all_rows.extend(chunk)
        if len(chunk) < page_size:
            break
    return all_rows


def _ssi_fc_item_close_volume(it: Dict[str, Any]) -> tuple[Optional[float], float]:
    """Lấy giá đóng + khối lượng từ một bản ghi DailyStockPrice (tên field khác nhau theo bản API)."""
    close: Optional[float] = None
    for k in (
        "ClosePrice",
        "closePrice",
        "Close",
        "close",
        "PriceClose",
        "MatchedPrice",
        "MatchPrice",
        "ClosingPrice",
        "MatClose",
        "matClose",
        "CloseIndex",
        "closeIndex",
        "IndexClose",
        "indexClose",
    ):
        if k in it:
            close = _parse_sci_float(it.get(k))
            if close is not None and close > 0:
                break
    if close is None:
        for k, v in it.items():
            lk = str(k).lower().replace(" ", "")
            if lk in ("closeprice", "close", "matchprice", "closingprice") and v is not None:
                close = _parse_sci_float(v)
                if close is not None and close > 0:
                    break
    vol = 0.0
    for k in (
        "TotalVolume",
        "totalVolume",
        "Volume",
        "volume",
        "TotalVol",
        "totalVol",
        "TradingVolume",
        "TotalTradedQty",
        "TotalTradingVolume",
    ):
        if k in it:
            vv = _parse_sci_float(it.get(k))
            if vv is not None and vv >= 0:
                vol = float(vv)
                break
    return close, vol


def _ssi_vnindex_bars_from_fastconnect(days: int) -> Optional[List[Dict[str, float]]]:
    """
    Chuỗi nến VN-Index qua SSI FastConnect DailyStockPrice (symbol VNINDEX).
    Không dùng quota vnstock; cần SSI_FC_CONSUMER_ID / SSI_FC_CONSUMER_SECRET.
    """
    import datetime as _dt

    if days <= 0:
        days = 260
    consumer_id, consumer_secret = _ssi_env_consumer_credentials()
    if not consumer_id or not consumer_secret:
        return None
    base_url = os.environ.get("SSI_FC_BASE_URL", "https://fc-data.ssi.com.vn")
    token = _ssi_get_access_token(base_url, consumer_id, consumer_secret)
    if not token:
        return None

    today = _dt.date.today()
    cal_span = max(int(days) * 2 + 120, 400)
    start_d = today - _dt.timedelta(days=cal_span)
    idx_sym = os.environ.get("SSI_FC_VNINDEX_SYMBOL", "VNINDEX").strip() or "VNINDEX"
    items = _ssi_collect_daily_stock_price_paged(
        base_url=base_url,
        token=token,
        symbol=idx_sym,
        from_date=start_d,
        to_date=today,
    )
    if not items:
        return None

    rows: List[tuple[_dt.date, float, float]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        td = it.get("TradingDate") or it.get("tradingDate") or it.get("Trading_date")
        if not td:
            continue
        d = _parse_fc_trading_date(str(td))
        if not d:
            continue
        c, v = _ssi_fc_item_close_volume(it)
        if c is None or c <= 0:
            continue
        rows.append((d, float(c), float(v)))

    rows.sort(key=lambda x: x[0])
    if len(rows) < 20:
        return None
    bars = [{"close": r[1], "volume": r[2]} for r in rows]
    return bars[-days:] if len(bars) > days else bars


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


def _build_moneyflow_response(
    unique: List[str], total_days: int, trend_sessions: int
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    SSI FastConnect + fallback vnstock_data. Dùng chung cho FastAPI và Vercel handler.
    """
    consumer_id, consumer_secret = _ssi_env_consumer_credentials()
    ssi_base_url = os.environ.get("SSI_FC_BASE_URL", "https://fc-data.ssi.com.vn")
    token = _ssi_get_access_token(
        base_url=ssi_base_url,
        consumer_id=consumer_id,
        consumer_secret=consumer_secret,
    )

    ssi_by_symbol: Dict[str, Dict[str, Any]] = {}
    if token and unique:
        workers = min(8, max(1, len(unique)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for symbol in unique:
                cache_key = f"{symbol}|{total_days}|{trend_sessions}"
                cached = _cache_get(_moneyflow_cache, cache_key)
                if cached is not None:
                    ssi_by_symbol[symbol] = cached
                    continue
                fut = pool.submit(
                    _compute_money_flow_for_symbol,
                    symbol,
                    total_days,
                    trend_sessions,
                    ssi_base_url,
                    token,
                )
                futures[fut] = (symbol, cache_key)
            for fut, (sym, cache_key) in futures.items():
                try:
                    res = fut.result(timeout=90)
                    if res:
                        ssi_by_symbol[sym] = res
                        _cache_set(
                            _moneyflow_cache,
                            cache_key,
                            res,
                            _MONEYFLOW_CACHE_TTL_SECONDS,
                        )
                except Exception:
                    continue

    out: Dict[str, Any] = {}
    for sym in unique:
        ssi_res = ssi_by_symbol.get(sym)
        if _moneyflow_api_totals_nonzero(ssi_res):
            out[sym] = ssi_res
            continue
        vn_flat = _get_moneyflow(sym, days=total_days)
        vn_payload = _vnstock_moneyflow_to_api_shape(vn_flat or {}, total_days)
        if vn_payload:
            out[sym] = vn_payload
        elif ssi_res:
            out[sym] = ssi_res

    debug = {
        "requested_tickers": unique,
        "totalDays": total_days,
        "trendSessions": trend_sessions,
        "trading_available": _Trading is not None,
        "ssi_token_available": bool(token),
    }
    return out, debug


@app.post("/api/moneyflow")
@app.post("/moneyflow")
def api_moneyflow(req: MoneyFlowRequest):
    """
    POST /api/moneyflow

    Ưu tiên SSI FastConnect (chuỗi theo phiên + tổng cửa sổ). Nếu không có token SSI,
    hoặc từng mã không có số liệu khả dụng từ SSI, fallback qua vnstock_data (VCI → TCBS → SSI).
    """
    tickers = req.tickers or []
    raw_td = req.days if req.days is not None else req.totalDays
    try:
        total_days = int(raw_td or 30)
    except Exception:
        total_days = 30
    total_days = max(1, total_days)
    try:
        trend_sessions = int(req.trendSessions or 20)
    except Exception:
        trend_sessions = 20
    trend_sessions = max(1, trend_sessions)

    unique = list({str(t).strip().upper() for t in tickers if t})
    out, debug = _build_moneyflow_response(unique, total_days, trend_sessions)
    return JSONResponse(content={"data": out, "_debug": debug})


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


class MarketBatchRequest(BaseModel):
    tickers: List[str] = []
    includeFundamentals: bool = True
    includeMoneyFlow: bool = True
    totalDays: int = 30
    trendSessions: int = 20


@app.post("/api/market-batch")
def api_market_batch(req: MarketBatchRequest):
    """
    Gom dữ liệu fundamentals + moneyflow trong 1 request để giảm số lần gọi API.
    Response:
      {
        "data": {
          "MBB": {
            "fundamentals": {...} | null,
            "moneyFlow": {...} | null
          }
        }
      }
    """
    tickers = req.tickers or []
    unique = list({str(t).strip().upper() for t in tickers if t})

    include_f = bool(req.includeFundamentals)
    include_m = bool(req.includeMoneyFlow)
    total_days = int(req.totalDays or 30)
    trend_sessions = int(req.trendSessions or 20)

    out: Dict[str, Dict[str, Any]] = {
        t: {"fundamentals": None, "moneyFlow": None} for t in unique
    }

    # 1) fundamentals
    if include_f:
        for symbol in unique:
            cache_key = f"{symbol}|{','.join(SOURCES)}|heavy:{int(_ENABLE_HEAVY_FIELDS)}"
            cached = _cache_get(_fundamentals_cache, cache_key)
            if cached is not None:
                out[symbol]["fundamentals"] = cached
                continue
            try:
                item = _extract_for_sources(symbol, SOURCES)
                if any(x is not None for x in item.values()):
                    cleaned = {k: v for k, v in item.items() if v is not None}
                    out[symbol]["fundamentals"] = cleaned
                    _cache_set(
                        _fundamentals_cache,
                        cache_key,
                        cleaned,
                        _FUNDAMENTALS_CACHE_TTL_SECONDS,
                    )
            except Exception:
                continue

    # 2) moneyflow: SSI FastConnect (nếu có token) + fallback vnstock_data cho từng mã thiếu số liệu
    if include_m and unique:
        consumer_id, consumer_secret = _ssi_env_consumer_credentials()
        ssi_base_url = os.environ.get("SSI_FC_BASE_URL", "https://fc-data.ssi.com.vn")
        token = _ssi_get_access_token(
            base_url=ssi_base_url,
            consumer_id=consumer_id,
            consumer_secret=consumer_secret,
        )
        if token:
            workers = min(8, max(1, len(unique)))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {}
                for symbol in unique:
                    cache_key = f"{symbol}|{total_days}|{trend_sessions}"
                    cached = _cache_get(_moneyflow_cache, cache_key)
                    if cached is not None:
                        out[symbol]["moneyFlow"] = cached
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

                for fut, (symbol, cache_key) in futures.items():
                    try:
                        res = fut.result(timeout=90)
                        if res:
                            out[symbol]["moneyFlow"] = res
                            _cache_set(
                                _moneyflow_cache,
                                cache_key,
                                res,
                                _MONEYFLOW_CACHE_TTL_SECONDS,
                            )
                    except Exception:
                        continue

        for symbol in unique:
            mf_cur = out[symbol].get("moneyFlow")
            if _moneyflow_api_totals_nonzero(mf_cur):
                continue
            vn_flat = _get_moneyflow(symbol, days=total_days)
            vn_payload = _vnstock_moneyflow_to_api_shape(vn_flat or {}, total_days)
            if vn_payload:
                out[symbol]["moneyFlow"] = vn_payload

    return JSONResponse(content={"data": out})


# Vercel: entry `handler` — subclass BaseHTTPRequestHandler (do_POST / do_OPTIONS).
class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
            payload = json.loads(body) if body.strip() else {}
            tickers = payload.get("tickers") or []
        except Exception:
            tickers = []
            payload = {}

        unique = list({str(t).strip().upper() for t in tickers if t})
        path = (getattr(self, "path", "") or "").lower()

        if "moneyflow" in path:
            raw_td = payload.get("days")
            if raw_td is None:
                raw_td = payload.get("totalDays")
            try:
                total_days = int(raw_td or 30)
            except Exception:
                total_days = 30
            total_days = max(1, total_days)
            try:
                trend_sessions = int(payload.get("trendSessions") or 20)
            except Exception:
                trend_sessions = 20
            trend_sessions = max(1, trend_sessions)

            out_data, debug = _build_moneyflow_response(unique, total_days, trend_sessions)
            out_json = json.dumps(
                {"data": out_data, "_debug": debug}, ensure_ascii=False
            )
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
