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

import pandas as pd
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


class HistoryRequest(BaseModel):
    tickers: List[str] = []
    days: int = 260


class CompanyCatalystRequest(BaseModel):
    tickers: List[str] = []


# Có thể cấu hình nhiều nguồn, phân tách bằng dấu phẩy, ví dụ: "KBS,VCI".
# Lưu ý: vnstock 3.4+ chỉ hỗ trợ "KBS" và "VCI" cho Finance — các giá trị khác
# (SSI / CAFE / TCBS / MSN) sẽ ném ValueError trong Finance.__init__ và bị catch
# silently, dẫn đến /api/fundamentals trả về {"data":{}}.
_VALID_FINANCE_SOURCES = {"KBS", "VCI"}
_RAW_SOURCES = os.environ.get("VNSTOCK_SOURCE", "KBS,VCI")
SOURCES = [
    s.strip().upper()
    for s in _RAW_SOURCES.split(",")
    if s.strip() and s.strip().upper() in _VALID_FINANCE_SOURCES
]
if not SOURCES:
    SOURCES = ["KBS", "VCI"]

# Mặc định tắt field "nặng" để giảm số request/ticker khi dùng Guest plan.
# Có thể bật lại bằng FUNDAMENTALS_ENABLE_HEAVY_FIELDS=1.
_ENABLE_HEAVY_FIELDS = (
    str(os.environ.get("FUNDAMENTALS_ENABLE_HEAVY_FIELDS", "0")).strip().lower()
    in ("1", "true", "yes", "on")
)

# Fundamentals (PE/PB/ROE/EPS theo năm) đổi theo quý → cache dài (mặc định 6h)
# để giảm tải upstream và sống sót qua cold-start. Vẫn override được qua env.
_FUNDAMENTALS_CACHE_TTL_SECONDS = max(
    10, int(os.environ.get("FUNDAMENTALS_CACHE_TTL_SECONDS", "21600"))
)

# Ngân sách thời gian phía server, đặt DƯỚI mốc abort 25s của frontend
# (market-api.ts) để luôn kịp trả lời (kèm stale cache) thay vì để client timeout.
_FUNDAMENTALS_TOTAL_BUDGET_SECONDS = float(
    os.environ.get("FUNDAMENTALS_TOTAL_BUDGET_SECONDS", "18")
)
_FUNDAMENTALS_PER_TICKER_TIMEOUT = float(
    os.environ.get("FUNDAMENTALS_PER_TICKER_TIMEOUT", "8")
)
_FUNDAMENTALS_FETCH_WORKERS = max(
    1, int(os.environ.get("FUNDAMENTALS_FETCH_WORKERS", "4"))
)
_MONEYFLOW_FUTURE_TIMEOUT = float(os.environ.get("MONEYFLOW_FUTURE_TIMEOUT", "18"))
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
# Catalyst/ownership (news, events, shareholders, officers, insider) đổi chậm →
# cache dài 6h/ticker. Khi rate-limited thì serve stale (include_expired=True).
_company_catalyst_cache: Dict[str, Dict[str, Any]] = {}
_COMPANY_CATALYST_CACHE_TTL_SECONDS = 21600

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

# Initialize rate limiter. Mặc định 30/phút (an toàn dưới 60/phút của Community
# tier khi đã set VNSTOCK_API_KEY). Guest tier nên set env về 10.
_vnstock_limiter = VnstockRateLimiter(
    max_calls=int(os.environ.get("VNSTOCK_MAX_CALLS_PER_MINUTE", "30")),
    time_window=60
)


def _rl_call(func, *, label: str = "vnstock", max_wait: float = 3.0, cooldown: int = 60):
    """Gọi 1 hàm vnstock qua rate limiter DÙNG CHUNG cho toàn service.

    - Chờ tối đa `max_wait`s nếu sắp chạm limit; nếu phải chờ lâu hơn → raise
      RuntimeError để caller bỏ qua call này và rơi về cache (tránh hammer upstream).
    - Ghi nhận call thành công để limiter đếm chính xác.
    - Phát hiện lỗi 'rate limit / too many / 429' → bật cooldown để các call sau
      né upstream ngay, thay vì đập tiếp và bị khoá lâu hơn.
    """
    if not _vnstock_limiter.wait_if_needed(max_wait=max_wait):
        raise RuntimeError(f"rate_limited_skip:{label}")
    try:
        result = func()
        _vnstock_limiter.record_call()
        return result
    except Exception as e:
        msg = str(e).lower()
        if "rate limit" in msg or "too many" in msg or "429" in msg:
            _vnstock_limiter.set_rate_limited(cooldown)
        raise


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


def _cache_peek_fresh(cache: Dict[str, Dict[str, Any]], key: str) -> Optional[Any]:
    """Trả value nếu còn hạn; KHÔNG xoá entry hết hạn (để stale-while-revalidate
    còn dùng lại được). Khác với _cache_get vốn xoá entry hết hạn."""
    now = time.time()
    with _cache_lock:
        entry = cache.get(key)
        if not entry or entry.get("expires_at", 0) <= now:
            return None
        return entry.get("value")


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        x = float(value)
        return x if (x == x and abs(x) < 1e15) else None
    except (TypeError, ValueError):
        return None


# Map item_id từ vnstock ratio() -> field output (pe, pb, roe, eps).
# Lưu ý: vnstock 4.x (KBS source) trả `trailing_eps` thay vì `eps`.
# KHÔNG map `roe_trailling` vào `roe` vì KBS có CẢ hai item_id và row trailling
# thường rỗng cho kỳ hiện tại — sẽ ghi đè lên giá trị `roe` annual đúng.
_ITEM_ID_TO_FIELD = {
    "pe": "pe", "pe_ratio": "pe", "p_e": "pe", "price_to_earning": "pe", "ty_le_pe": "pe",
    "pb": "pb", "pb_ratio": "pb", "p_b": "pb", "price_to_book": "pb", "ty_le_pb": "pb",
    "roe": "roe", "return_on_equity": "roe",
    "eps": "eps", "trailing_eps": "eps", "earnings_per_share": "eps",
    "loi_nhuan_tren_co_phieu": "eps",
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
            df = _rl_call(lambda: finance.ratio(period="year", lang="vi"), label="ratio")
        except TypeError:
            df = _rl_call(lambda: finance.ratio(period="year", display_mode="vi"), label="ratio")
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
            df = _rl_call(lambda: finance.cash_flow(period="year", lang="vi"), label="cash_flow")
        except TypeError:
            df = _rl_call(lambda: finance.cash_flow(period="year", display_mode="vi"), label="cash_flow")
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
    """Từ DataFrame ratio (mỗi hàng = một chỉ số), trích pe, pb, roe, eps từ cột kỳ mới nhất.

    First-non-None wins: nếu nhiều item_id cùng map vào 1 field (vd KBS có cả
    `roe` và `roe_trailling`), giữ giá trị xuất hiện trước và bỏ qua row sau —
    tránh trailling row rỗng ghi đè lên annual đúng.
    """
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
        if out[field] is not None:
            continue  # đã có giá trị từ row trước, không ghi đè
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
            fr_df = _rl_call(lambda: trading.foreign_trade(start=start_str, end=end_str), label="foreign_trade")
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
            prop_df = _rl_call(lambda: trading.prop_trade(start=start_str, end=end_str, resolution="1D"), label="prop_trade")
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
        df = _rl_call(lambda: company.overview(), label="overview")
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
            df = _rl_call(lambda: quote.history(length="1Y", interval="1D"), label="volume_ma")
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
        ranges = ("10y", "5y")
    elif days > 220:
        ranges = ("10y", "5y", "2y", "1y")
    else:
        ranges = ("5y", "2y", "1y")
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


def _vnindex_prefer_domestic() -> bool:
    """
    Mặc định bật: SSI FC → vnstock → Yahoo → Robot (gần HOSE hơn).
    Tắt: VNINDEX_PREFER_DOMESTIC=0|false — Yahoo trước (môi trường không gọi được SSI/vnstock).
    """
    v = str(os.environ.get("VNINDEX_PREFER_DOMESTIC", "1")).strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    return True


def _vnindex_bars_try_robotstock(days: int) -> Optional[List[Dict[str, float]]]:
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
        if not isinstance(rows, list) or len(rows) < 20:
            return None
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
        if len(bars_rs) < 20:
            return None
        if max(b["close"] for b in bars_rs) > 5000:
            for b in bars_rs:
                b["close"] /= 50.0
        return bars_rs[-days:] if len(bars_rs) > days else bars_rs
    except Exception:
        return None


def _vnindex_bars_try_vnstock_paths(
    days: int,
    skip_vn: bool,
    stale_any: Optional[List[Dict[str, float]]],
    bars_from_df: Any,
    on_quota_exit: Any,
) -> tuple[Optional[List[Dict[str, float]]], bool]:
    """
    Thử toàn bộ nhánh vnstock cho VNINDEX.
    Trả về (bars, True) nếu caller nên return stale_any (quota + có stale).
    """
    if skip_vn:
        return None, False

    if Quote is not None:
        for source in ("KBS", "TCBS", "DNSE"):
            if not _vnstock_limiter.wait_if_needed(max_wait=3.0):
                break
            try:
                quote = Quote(symbol="VNINDEX", source=source)
                df = _vnstock_safe_quote_history(quote, length="1Y", interval="1D")
                if df is None:
                    on_quota_exit()
                    if stale_any is not None:
                        return None, True
                    break
                _vnstock_limiter.record_call()
                bars = bars_from_df(df)
                if bars:
                    return bars, False
            except Exception as e:
                error_msg = str(e).lower()
                if "rate limit" in error_msg or "too many" in error_msg:
                    _vnstock_limiter.set_rate_limited(60)
                    if stale_any is not None:
                        return None, True
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
                    on_quota_exit()
                else:
                    _vnstock_limiter.record_call()
                    bars = bars_from_df(df)
                    if bars:
                        return bars, False
        except Exception:
            pass

    if get_index_series is not None and _vnstock_limiter.wait_if_needed(max_wait=3.0):
        try:
            try:
                df = get_index_series(index_code="VNINDEX", time_range="OneYear")
            except SystemExit:
                df = None
                on_quota_exit()
            if df is not None:
                _vnstock_limiter.record_call()
                bars = bars_from_df(df)
                if bars:
                    return bars, False
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
                            on_quota_exit()
                        if df is None:
                            continue
                        _vnstock_limiter.record_call()
                        bars = bars_from_df(df)
                        if bars:
                            return bars, False
                    except TypeError:
                        try:
                            try:
                                df = stock_historical_data("VNINDEX", start_str, end_str)
                            except SystemExit:
                                df = None
                                on_quota_exit()
                            if df is None:
                                continue
                            _vnstock_limiter.record_call()
                            bars = bars_from_df(df)
                            if bars:
                                return bars, False
                        except Exception:
                            pass
        except Exception:
            pass

    return None, False


def _get_vnindex_bars(days: int = 260) -> Optional[List[Dict[str, float]]]:
    """
    Chuỗi nến daily VN-Index (cũ → mới): close, volume (0 nếu nguồn không có).

    Thứ tự nguồn:
    - Mặc định (VNINDEX_PREFER_DOMESTIC=1 hoặc unset): SSI FC → vnstock → Yahoo → Robot.
    - VNINDEX_PREFER_DOMESTIC=0: Yahoo → SSI FC → Robot → vnstock.
    """
    if days <= 0:
        days = 260
    
    # Try cache first
    cache_key = f"vnindex_bars_{days}"
    cached = _cache_get(_vnindex_cache, cache_key)
    if cached is not None:
        return cached

    stale_any = _cache_get(_vnindex_cache, cache_key, include_expired=True)

    def _save_return(bars: List[Dict[str, float]]) -> List[Dict[str, float]]:
        _cache_set(_vnindex_cache, cache_key, bars, _VNINDEX_CACHE_TTL_SECONDS)
        return bars

    prefer_domestic = _vnindex_prefer_domestic()

    # Đang cooldown vnstock: vẫn thử SSI / Yahoo (không đụng quota vnai).
    if _vnstock_limiter.is_rate_limited():
        if prefer_domestic:
            bars_s = _ssi_vnindex_bars_from_fastconnect(days)
            if bars_s:
                return _save_return(bars_s)
        bars_y = _yahoo_fetch_vnindex_bars(days)
        if bars_y:
            return _save_return(bars_y)
        if not prefer_domestic:
            bars_s = _ssi_vnindex_bars_from_fastconnect(days)
            if bars_s:
                return _save_return(bars_s)
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

    if prefer_domestic:
        # SSI FastConnect → vnstock → Yahoo → Robot (chỉ số gần HOSE / trong nước)
        bars_fc = _ssi_vnindex_bars_from_fastconnect(days)
        if bars_fc:
            return _save_return(bars_fc)
        vbars, need_stale = _vnindex_bars_try_vnstock_paths(
            days, skip_vn, stale_any, _bars_from_df, _on_vnstock_quota_exit
        )
        if need_stale:
            return stale_any
        if vbars:
            return _save_return(vbars)
        bars_yahoo = _yahoo_fetch_vnindex_bars(days)
        if bars_yahoo:
            return _save_return(bars_yahoo)
        bars_robot = _vnindex_bars_try_robotstock(days)
        if bars_robot:
            return _save_return(bars_robot)
        return stale_any

    # Mặc định: Yahoo (ổn trên cloud không SSI) → SSI → Robot → vnstock
    bars_yahoo = _yahoo_fetch_vnindex_bars(days)
    if bars_yahoo:
        return _save_return(bars_yahoo)

    bars_fc = _ssi_vnindex_bars_from_fastconnect(days)
    if bars_fc:
        return _save_return(bars_fc)

    bars_robot = _vnindex_bars_try_robotstock(days)
    if bars_robot:
        return _save_return(bars_robot)

    if skip_vn:
        return stale_any

    vbars, need_stale = _vnindex_bars_try_vnstock_paths(
        days, skip_vn, stale_any, _bars_from_df, _on_vnstock_quota_exit
    )
    if need_stale:
        return stale_any
    if vbars:
        return _save_return(vbars)

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
    # Yahoo / một số API index trả thanh khoản kiểu ~5M (không phải CP toàn sàn) — ẩn thay vì hiển thị sai.
    try:
        tv_raw = vol_info.get("volume_today")
        thr = float(
            os.environ.get("VNINDEX_INDEX_VOLUME_MIN_TRUST_SHARES", "40000000")
        )
        if (
            last > 1500
            and tv_raw is not None
            and float(tv_raw) > 0
            and float(tv_raw) < thr
        ):
            vol_info = {
                "volume_today": None,
                "volume_avg20": None,
                "volume_vs_avg20_pct": None,
            }
    except (TypeError, ValueError):
        pass

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


@app.post("/api/history")
@app.post("/history")
def api_history(req: HistoryRequest):
    """Chuỗi giá lịch sử daily (close + volume), cũ → mới.

    - **VNINDEX**: dùng `_get_vnindex_bars` (đa nguồn SSI FC / vnstock / Yahoo / Robot,
      có cache + cooldown-aware) → bền hơn 1 nguồn Yahoo (vốn hay 429).
    - **Mã lẻ**: `_get_equity_close_prices` (vnstock) — fallback khi Yahoo phía client lỗi.

    Shape khớp client (market-api.ts fetchHistoricalDataViaBackend):
    `{"data": {SYM: {"close": [...], "volume": [...]}}}`.
    """
    days = int(req.days or 260)
    if days <= 0:
        days = 260
    out: Dict[str, Any] = {}
    for raw in (req.tickers or []):
        sym = str(raw).strip().upper()
        if not sym:
            continue
        try:
            if sym in ("VNINDEX", "^VNINDEX", "VN-INDEX", "VNI"):
                bars = _get_vnindex_bars(days) or []
                out["VNINDEX"] = {
                    "close": [
                        float(b["close"]) for b in bars if b.get("close") is not None
                    ],
                    "volume": [float(b.get("volume") or 0.0) for b in bars],
                }
            else:
                closes = _get_equity_close_prices(sym, days) or []
                out[sym] = {"close": [float(c) for c in closes], "volume": []}
        except Exception as e:
            print(f"[history] {sym} EXCEPTION: {type(e).__name__}: {e}", flush=True)
            continue
    return JSONResponse(content={"data": out})


def _catalyst_date_str(value: Any) -> Optional[str]:
    """Coerce pandas Timestamp / datetime / str về 'YYYY-MM-DD' an toàn."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if ts is None or pd.isna(ts):
        return None
    try:
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return None


def _catalyst_to_pct(value: Any, max_seen: float) -> Optional[float]:
    """Chuẩn hoá own_percent về PHẦN TRĂM. Nhân 100 chỉ khi cả cột là fraction
    (max <= 1.5), tránh nhân nhầm khi nguồn đã trả percent (vd 5.0)."""
    v = _safe_float(value)
    if v is None:
        return None
    if max_seen <= 1.5:
        v = v * 100.0
    return v


def _catalyst_news(company) -> List[Dict[str, Any]]:
    """Lên tới 5 tin mới nhất, chỉ giữ public_date trong vòng 60 ngày."""
    try:
        df = _rl_call(lambda: company.news(), label="catalyst_news")
    except Exception as e:
        print(f"[catalyst] news EXCEPTION: {type(e).__name__}: {e}", flush=True)
        return []
    if df is None or getattr(df, "empty", True):
        return []
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=60)
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        raw_date = row.get("public_date")
        ts = pd.to_datetime(raw_date, errors="coerce")
        if ts is None or pd.isna(ts) or ts.normalize() < cutoff:
            continue
        title = row.get("news_title")
        title = str(title)[:200] if title is not None and not pd.isna(title) else None
        src = row.get("news_source")
        src = str(src) if src is not None and not pd.isna(src) else None
        rows.append({
            "title": title,
            "date": _catalyst_date_str(raw_date),
            "source": src,
            "_sort": ts,
        })
    rows.sort(key=lambda r: r["_sort"], reverse=True)
    out: List[Dict[str, Any]] = []
    for r in rows[:5]:
        r.pop("_sort", None)
        out.append(r)
    return out


def _catalyst_events(company) -> List[Dict[str, Any]]:
    """Lên tới 6 sự kiện; ưu tiên record_date/exright_date trong tương lai HOẶC
    trong vòng 30 ngày gần đây; sắp xếp theo public_date mới nhất trước."""
    try:
        df = _rl_call(lambda: company.events(), label="catalyst_events")
    except Exception as e:
        print(f"[catalyst] events EXCEPTION: {type(e).__name__}: {e}", flush=True)
        return []
    if df is None or getattr(df, "empty", True):
        return []
    today = pd.Timestamp.now().normalize()
    recent_cutoff = today - pd.Timedelta(days=30)
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        rec_ts = pd.to_datetime(row.get("record_date"), errors="coerce")
        exr_ts = pd.to_datetime(row.get("exright_date"), errors="coerce")
        relevant = False
        for ts in (rec_ts, exr_ts):
            if ts is not None and not pd.isna(ts):
                d = ts.normalize()
                if d >= recent_cutoff:  # tương lai hoặc trong 30 ngày gần đây
                    relevant = True
                    break
        pub_ts = pd.to_datetime(row.get("public_date"), errors="coerce")
        title = row.get("event_title_vi") or row.get("event_name_vi")
        title = str(title)[:200] if title is not None and not pd.isna(title) else None
        action = row.get("action_type_vi")
        action = str(action) if action is not None and not pd.isna(action) else None
        ratio = row.get("exercise_ratio")
        ratio = str(ratio) if ratio is not None and not pd.isna(ratio) else None
        rows.append({
            "title": title,
            "actionType": action,
            "publicDate": _catalyst_date_str(row.get("public_date")),
            "recordDate": _catalyst_date_str(row.get("record_date")),
            "exrightDate": _catalyst_date_str(row.get("exright_date")),
            "valuePerShare": _safe_float(row.get("value_per_share")),
            "exerciseRatio": ratio,
            "_relevant": relevant,
            "_sort": pub_ts if (pub_ts is not None and not pd.isna(pub_ts)) else pd.Timestamp.min,
        })
    # Ưu tiên relevant trước, trong mỗi nhóm sắp theo public_date mới nhất.
    rows.sort(key=lambda r: (r["_relevant"], r["_sort"]), reverse=True)
    out: List[Dict[str, Any]] = []
    for r in rows[:6]:
        r.pop("_relevant", None)
        r.pop("_sort", None)
        out.append(r)
    return out


def _catalyst_ownership(company) -> Dict[str, Any]:
    """top5Pct (tổng % của 5 cổ đông lớn nhất), officersPct (tổng % ban lãnh đạo),
    topHolders (tối đa 5, sắp giảm dần). Mọi % chuẩn hoá về PHẦN TRĂM."""
    out: Dict[str, Any] = {"top5Pct": None, "officersPct": None, "topHolders": []}
    # Shareholders
    try:
        sdf = _rl_call(lambda: company.shareholders(), label="catalyst_shareholders")
    except Exception as e:
        print(f"[catalyst] shareholders EXCEPTION: {type(e).__name__}: {e}", flush=True)
        sdf = None
    if sdf is not None and not getattr(sdf, "empty", True) and "share_own_percent" in sdf.columns:
        pct_vals = [_safe_float(v) for v in sdf["share_own_percent"].tolist()]
        pct_vals_clean = [v for v in pct_vals if v is not None]
        max_seen = max(pct_vals_clean) if pct_vals_clean else 0.0
        holders: List[Dict[str, Any]] = []
        for _, row in sdf.iterrows():
            name = row.get("share_holder")
            name = str(name) if name is not None and not pd.isna(name) else None
            pct = _catalyst_to_pct(row.get("share_own_percent"), max_seen)
            if name is None or pct is None:
                continue
            holders.append({"name": name, "pct": pct})
        holders.sort(key=lambda h: h["pct"], reverse=True)
        top5 = holders[:5]
        if top5:
            out["top5Pct"] = round(sum(h["pct"] for h in top5), 2)
        out["topHolders"] = [{"name": h["name"], "pct": round(h["pct"], 2)} for h in top5]
    # Officers
    try:
        odf = _rl_call(lambda: company.officers(), label="catalyst_officers")
    except Exception as e:
        print(f"[catalyst] officers EXCEPTION: {type(e).__name__}: {e}", flush=True)
        odf = None
    if odf is not None and not getattr(odf, "empty", True) and "officer_own_percent" in odf.columns:
        opct_vals = [_safe_float(v) for v in odf["officer_own_percent"].tolist()]
        opct_clean = [v for v in opct_vals if v is not None]
        o_max = max(opct_clean) if opct_clean else 0.0
        total = 0.0
        any_val = False
        for v in odf["officer_own_percent"].tolist():
            p = _catalyst_to_pct(v, o_max)
            if p is not None:
                total += p
                any_val = True
        if any_val:
            out["officersPct"] = round(total, 2)
    return out


def _catalyst_insider(company) -> List[Dict[str, Any]]:
    """insider_trading() CHƯA verify → gọi phòng thủ; lỗi/rỗng → []. Tối đa 5 mới nhất."""
    try:
        df = _rl_call(lambda: company.insider_trading(), label="catalyst_insider")
    except Exception as e:
        print(f"[catalyst] insider EXCEPTION: {type(e).__name__}: {e}", flush=True)
        return []
    if df is None or getattr(df, "empty", True):
        return []
    try:
        cols = {c.lower(): c for c in df.columns}
        def pick(*names):
            for n in names:
                if n in cols:
                    return cols[n]
            return None
        person_col = pick("person", "owner_name", "name", "share_holder", "officer_name")
        action_col = pick("action", "action_type", "deal_type", "transaction", "type")
        qty_col = pick("quantity", "volume", "shares", "deal_volume", "quantity_change")
        date_col = pick("date", "public_date", "deal_date", "trade_date", "update_date")
        rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            ts = pd.to_datetime(row.get(date_col), errors="coerce") if date_col else None
            rows.append({
                "person": (str(row.get(person_col)) if person_col and row.get(person_col) is not None and not pd.isna(row.get(person_col)) else None),
                "action": (str(row.get(action_col)) if action_col and row.get(action_col) is not None and not pd.isna(row.get(action_col)) else None),
                "quantity": _safe_float(row.get(qty_col)) if qty_col else None,
                "date": _catalyst_date_str(row.get(date_col)) if date_col else None,
                "_sort": ts if (ts is not None and not pd.isna(ts)) else pd.Timestamp.min,
            })
        rows.sort(key=lambda r: r["_sort"], reverse=True)
        out: List[Dict[str, Any]] = []
        for r in rows[:5]:
            r.pop("_sort", None)
            out.append(r)
        return out
    except Exception as e:
        print(f"[catalyst] insider PARSE EXCEPTION: {type(e).__name__}: {e}", flush=True)
        return []


def _get_company_catalyst(symbol: str) -> Dict[str, Any]:
    """News + events + ownership + insider của 1 mã (cache 6h, rate-limit guard).

    Cache-first; nếu rate-limited thì serve stale (include_expired=True). Mỗi method
    vnstock gọi qua _rl_call và bọc try/except riêng → một method lỗi không kéo đổ
    cả response. Company is None (import fail) → trả shape rỗng graceful.
    """
    empty: Dict[str, Any] = {
        "news": [],
        "events": [],
        "ownership": {"top5Pct": None, "officersPct": None, "topHolders": []},
        "insiderTrading": [],
    }

    cache_key = symbol
    cached = _cache_get(_company_catalyst_cache, cache_key)
    if cached is not None:
        return cached

    if Company is None:
        return empty

    # Rate-limited → serve stale nếu có, tránh hammer upstream.
    if _vnstock_limiter.is_rate_limited():
        stale = _cache_get(_company_catalyst_cache, cache_key, include_expired=True)
        if stale is not None:
            return stale
        return empty

    try:
        try:
            company = Company(symbol=symbol, source="VCI")
        except TypeError:
            company = Company(symbol=symbol)
    except Exception as e:
        print(f"[catalyst] {symbol} Company init EXCEPTION: {type(e).__name__}: {e}", flush=True)
        stale = _cache_get(_company_catalyst_cache, cache_key, include_expired=True)
        return stale if stale is not None else empty

    result: Dict[str, Any] = {
        "news": _catalyst_news(company),
        "events": _catalyst_events(company),
        "ownership": _catalyst_ownership(company),
        "insiderTrading": _catalyst_insider(company),
    }

    _cache_set(_company_catalyst_cache, cache_key, result, _COMPANY_CATALYST_CACHE_TTL_SECONDS)
    return result


@app.post("/api/company-catalyst")
@app.post("/company-catalyst")
def api_company_catalyst(req: CompanyCatalystRequest):
    out: Dict[str, Any] = {}
    for raw in (req.tickers or [])[:30]:   # cap 30 tickers
        sym = str(raw).strip().upper()
        if not sym:
            continue
        try:
            out[sym] = _get_company_catalyst(sym)
        except Exception as e:
            print(f"[catalyst] {sym} EXCEPTION: {type(e).__name__}: {e}", flush=True)
            continue
    return JSONResponse(content={"data": out})


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
                    res = fut.result(timeout=_MONEYFLOW_FUTURE_TIMEOUT)
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


@app.get("/api/debug/extract")
def api_debug_extract(symbol: str, source: Optional[str] = None):
    """
    Endpoint chẩn đoán: chạy thử ratio()/overview()/cash_flow() cho 1 ticker và
    trả về exception/data thật của từng nguồn — phục vụ debug Render khi
    /api/fundamentals trả empty.

    Ví dụ: GET /api/debug/extract?symbol=VNM
           GET /api/debug/extract?symbol=VNM&source=KBS
    """
    import traceback as _tb
    sym = (symbol or "").strip().upper()
    if not sym:
        return JSONResponse(content={"error": "symbol required"}, status_code=400)
    sources_to_try = [source.strip().upper()] if source else list(SOURCES)
    out: Dict[str, Any] = {
        "symbol": sym,
        "valid_finance_sources": sorted(_VALID_FINANCE_SOURCES),
        "configured_SOURCES": SOURCES,
        "sources_tried": sources_to_try,
        "results": {},
    }
    for src in sources_to_try:
        entry: Dict[str, Any] = {}
        try:
            df = _get_ratio_df(sym, src)
            if df is None:
                entry["ratio"] = "None (Finance/init or empty)"
            elif hasattr(df, "empty") and df.empty:
                entry["ratio"] = {"empty": True, "columns": list(df.columns)[:8]}
            else:
                ids = []
                if "item_id" in df.columns:
                    ids = df["item_id"].astype(str).tolist()[:60]
                entry["ratio"] = {
                    "shape": list(getattr(df, "shape", []) or []),
                    "columns": list(df.columns)[:8],
                    "item_ids": ids,
                    "parsed": _parse_ratio_df(df),
                }
        except Exception as e:
            entry["ratio_exception"] = f"{type(e).__name__}: {e}"
            entry["traceback"] = _tb.format_exc(limit=5)
        try:
            row = _get_overview_row(sym, src)
            entry["overview_keys"] = list(row.keys())[:30] if row else None
        except Exception as e:
            entry["overview_exception"] = f"{type(e).__name__}: {e}"
        out["results"][src] = entry
    return JSONResponse(content=out)


def _fundamentals_worker(symbol: str, cache_key: str) -> Optional[Dict[str, Any]]:
    """Fetch fundamentals cho 1 mã rồi tự cache khi thành công.

    Tự cache ngay trong worker để thread nào về muộn (sau khi main thread đã hết
    budget) vẫn làm ấm cache cho request kế tiếp — biến timeout thành cache-warm.
    """
    item = _extract_for_sources(symbol, SOURCES)
    if any(v is not None for v in item.values()):
        cleaned = {k: v for k, v in item.items() if v is not None}
        _cache_set(_fundamentals_cache, cache_key, cleaned, _FUNDAMENTALS_CACHE_TTL_SECONDS)
        return cleaned
    return None


def _fetch_fundamentals_map(
    unique: List[str],
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Lấy fundamentals cho nhiều mã: cache-first, fetch song song có giới hạn,
    ràng buộc tổng budget < abort của frontend, và stale-while-revalidate.

    Trả về (data, errors). Mã nào timeout/lỗi/empty mà còn cache cũ → trả cache cũ
    (kèm cờ _stale) thay vì để trống; chỉ vào `errors` khi không có gì để trả.
    """
    data: Dict[str, Dict[str, Any]] = {}
    errors: Dict[str, str] = {}
    to_fetch: List[tuple] = []

    for symbol in unique:
        cache_key = f"{symbol}|{','.join(SOURCES)}|heavy:{int(_ENABLE_HEAVY_FIELDS)}"
        # Peek không-xoá: giữ lại entry hết hạn để stale-while-revalidate dùng tiếp.
        fresh = _cache_peek_fresh(_fundamentals_cache, cache_key)
        if fresh is not None:
            data[symbol] = fresh
        else:
            to_fetch.append((symbol, cache_key))

    if not to_fetch:
        return data, errors

    def _fallback_stale(symbol: str, cache_key: str, reason: str) -> None:
        stale = _cache_get(_fundamentals_cache, cache_key, include_expired=True)
        if stale is not None:
            data[symbol] = {**stale, "_stale": True}
        else:
            errors[symbol] = reason
            print(f"[fundamentals] {symbol}: {reason}", flush=True)

    deadline = time.time() + _FUNDAMENTALS_TOTAL_BUDGET_SECONDS
    workers = min(_FUNDAMENTALS_FETCH_WORKERS, len(to_fetch))
    # KHÔNG dùng `with ... as pool`: context-exit gọi shutdown(wait=True) sẽ chặn
    # tới khi worker chậm chạy xong → phá vỡ ràng buộc budget. Tự shutdown(wait=False).
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = {
            pool.submit(_fundamentals_worker, sym, ck): (sym, ck)
            for sym, ck in to_fetch
        }
        for fut, (symbol, cache_key) in futures.items():
            remaining = max(0.0, deadline - time.time())
            # Luôn để 1 cửa sổ nhỏ để thu hoạch future đã xong dù budget đã cạn.
            per_call = min(_FUNDAMENTALS_PER_TICKER_TIMEOUT, remaining) if remaining > 0 else 0.05
            try:
                cleaned = fut.result(timeout=per_call)
                if cleaned:
                    data[symbol] = cleaned
                else:
                    _fallback_stale(symbol, cache_key, f"all_sources_empty (tried: {','.join(SOURCES)})")
            except Exception as e:
                # Timeout (budget) / lỗi upstream / rate-limit-skip → stale-while-revalidate.
                # Worker vẫn chạy nền và tự cache khi xong → làm ấm cho request kế tiếp.
                _fallback_stale(symbol, cache_key, f"{type(e).__name__}: {str(e)[:160]}")
    finally:
        # wait=False: trả lời ngay; worker chậm chạy tiếp ở nền (đã được rate-limit gate).
        pool.shutdown(wait=False, cancel_futures=True)

    return data, errors


@app.post("/api/fundamentals")
@app.post("/fundamentals")
@app.post("/")
def api_fundamentals(req: FundamentalsRequest):
    """
    FastAPI endpoint for fundamentals.

    Request body:
        {"tickers": ["SSI", "MBB", ...]}

    Response:
        {"data": {"SSI": {"pe", "pb", "roe", "eps"}, ...},
         "_errors": {"TICKER": "ExceptionType: msg"}  # chỉ có khi có lỗi
        }

    Khi field không có nguồn nào trả số → ticker xuất hiện trong `_errors` với
    note "all_sources_empty" để client phân biệt được "lỗi" vs "data thiếu".
    """
    tickers = req.tickers or []
    unique = list({str(t).strip().upper() for t in tickers if t})
    data, errors = _fetch_fundamentals_map(unique)
    body: Dict[str, Any] = {"data": data}
    if errors:
        body["_errors"] = errors
    return JSONResponse(content=body)


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

    # 1) fundamentals (cache-first, song song có giới hạn, stale-while-revalidate)
    errors_f: Dict[str, str] = {}
    if include_f:
        f_data, errors_f = _fetch_fundamentals_map(unique)
        for symbol, cleaned in f_data.items():
            out[symbol]["fundamentals"] = cleaned

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
                        res = fut.result(timeout=_MONEYFLOW_FUTURE_TIMEOUT)
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

    body: Dict[str, Any] = {"data": out}
    if errors_f:
        body["_errors"] = {"fundamentals": errors_f}
    return JSONResponse(content=body)


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

        data, errors = _fetch_fundamentals_map(unique)
        body: Dict[str, Any] = {"data": data}
        if errors:
            body["_errors"] = errors
        out = json.dumps(body, ensure_ascii=False)
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
