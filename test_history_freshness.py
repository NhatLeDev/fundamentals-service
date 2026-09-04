"""Chọn nguồn giá theo ĐỘ TƯƠI, không chỉ theo độ dài chuỗi.

═══ LỖI ĐANG VÁ ═══════════════════════════════════════════════════════════════════════════════
`_get_equity_ohlc` trước đây nhận nguồn ĐẦU TIÊN trả ≥200 nến; không dòng nào nhìn ngày nến cuối.
Nguồn đầu trễ một phiên là trả luôn, không bao giờ thử nguồn sau.

[ĐO] census 39 row cache `ohlc:v2:` phía client (04/09/2026): BAF và VCB được ghi lúc 03/09 10:27
với n=248 (dáng KBS) và nến cuối **28/08**, trong khi DGW và VNM ghi CÙNG PHÚT ĐÓ, cùng đường này,
lại có nến **03/09**. Cùng service, cùng phút, độ tươi khác nhau theo mã.

[ĐO] 04/09/2026 trên vnstock 4.0.7: `Quote` chỉ nhận KBS và VCI; TCBS ném ValueError trong
__init__, DNSE không có trong registry `['fmp','kbs','msn','vci']` (thử cả hoa lẫn thường).

═══ TEST NÀY **KHÔNG** CHỨNG MINH ═════════════════════════════════════════════════════════════
  - rằng prod đang chạy đúng vnstock 4.0.7 (requirements vừa ghim, service không phơi version);
  - rằng nguồn upstream sẽ hết trả chuỗi trễ — chỉ chứng minh service không còn nhận bừa nguồn đầu.

Chạy: python -m pytest test_history_freshness.py -q
"""
from __future__ import annotations

import sys
import types

import pandas as pd
import pytest

import main


def _df(dates: list[str]) -> pd.DataFrame:
    """DataFrame dáng vnstock: đủ ≥200 nến để qua ngưỡng độ dài, ngày cuối do test đặt."""
    pad = 210 - len(dates)
    all_dates = [f"2020-01-{(i % 28) + 1:02d}" for i in range(pad)] + dates
    n = len(all_dates)
    return pd.DataFrame(
        {
            "time": all_dates,
            "open": [10.0] * n,
            "high": [11.0] * n,
            "low": [9.0] * n,
            "close": [10.5] * n,
            "volume": [1000.0] * n,
        }
    )


@pytest.fixture(autouse=True)
def _no_rate_limit(monkeypatch):
    """Bỏ rate limiter khỏi phương trình — test này đo LOGIC CHỌN NGUỒN, không đo limiter."""
    monkeypatch.setattr(main, "_rl_call", lambda func, **kw: func())
    with main._cache_lock:
        main._vnindex_cache.clear()
        main._observed_session_dates.clear()
    yield
    with main._cache_lock:
        main._vnindex_cache.clear()
        main._observed_session_dates.clear()


def _stub_quote(monkeypatch, per_source: dict[str, list[str] | None]) -> list[str]:
    """Thay `Quote` bằng bản giả. Trả về list ghi lại THỨ TỰ nguồn đã được gọi."""
    called: list[str] = []

    class FakeQuote:
        def __init__(self, symbol: str, source: str):
            called.append(source)
            if source not in per_source:
                raise ValueError(f"Provider 'quote/{source}' not found")
            self._dates = per_source[source]

        def history(self, **kwargs):
            return None if self._dates is None else _df(self._dates)

    monkeypatch.setattr(main, "Quote", FakeQuote)
    return called


def _seed_vnindex(latest: str) -> None:
    main._cache_set(main._vnindex_cache, "vnindex_bars_260", [{"date": latest, "close": 1.0}], 300)


# ── mốc phiên tham chiếu ───────────────────────────────────────────────────────────────────────

def test_moc_lay_tu_cache_vnindex():
    _seed_vnindex("2026-09-03")
    assert main._latest_known_session_date() == "2026-09-03"


def test_chua_co_cache_thi_khong_doan():
    assert main._latest_known_session_date() is None


def test_cache_het_han_khong_duoc_dung_lam_moc():
    main._cache_set(main._vnindex_cache, "vnindex_bars_260", [{"date": "2026-09-03"}], 1)
    main._vnindex_cache["vnindex_bars_260"]["expires_at"] = 0.0
    assert main._latest_known_session_date() is None


def test_ohlc_last_date_bo_qua_o_trong():
    assert main._ohlc_last_date({"time": ["2026-09-01", "2026-09-03", ""]}) == "2026-09-03"
    assert main._ohlc_last_date({"time": []}) is None
    assert main._ohlc_last_date({}) is None


# ── chọn nguồn ─────────────────────────────────────────────────────────────────────────────────

def test_nguon_dau_tuoi_thi_KHONG_goi_them(monkeypatch):
    """Ca thường phải tốn 0 call thêm — nếu không thì bản vá tự biến thành nguồn tải mới."""
    _seed_vnindex("2026-09-03")
    called = _stub_quote(monkeypatch, {"KBS": ["2026-09-03"], "VCI": ["2026-09-03"]})
    out = main._get_equity_ohlc("HCM", 260)
    assert main._ohlc_last_date(out) == "2026-09-03"
    assert called == ["KBS"]


def test_nguon_dau_TRE_thi_sang_nguon_ke(monkeypatch):
    """Đúng ca BAF/VCB: KBS dừng ở 28/08 trong khi phiên 03/09 đã biết."""
    _seed_vnindex("2026-09-03")
    called = _stub_quote(monkeypatch, {"KBS": ["2026-08-28"], "VCI": ["2026-09-03"]})
    out = main._get_equity_ohlc("BAF", 260)
    assert main._ohlc_last_date(out) == "2026-09-03"
    assert called == ["KBS", "VCI"]


def test_moi_nguon_deu_tre_thi_tra_ban_TUOI_NHAT_chu_khong_tra_None(monkeypatch):
    """Fail-open: 'hơi cũ' vẫn hơn 'không có gì' — client đã có cổng UNMEASURED chặn phán quyết."""
    _seed_vnindex("2026-09-03")
    _stub_quote(monkeypatch, {"KBS": ["2026-08-27"], "VCI": ["2026-08-28"]})
    out = main._get_equity_ohlc("BAF", 260)
    assert out is not None
    assert main._ohlc_last_date(out) == "2026-08-28"


def test_khong_co_moc_thi_giu_HANH_VI_CU(monkeypatch):
    """Chưa có cache VN-Index → nhận nguồn đầu như trước, không tự tạo đường thất bại mới."""
    called = _stub_quote(monkeypatch, {"KBS": ["2026-08-28"], "VCI": ["2026-09-03"]})
    out = main._get_equity_ohlc("BAF", 260)
    assert main._ohlc_last_date(out) == "2026-08-28"
    assert called == ["KBS"]


def test_nguon_moi_hon_moc_van_duoc_nhan(monkeypatch):
    """Trong phiên, nguồn có nến đang chạy mới hơn mốc → nhận, KHÔNG coi là bất thường."""
    _seed_vnindex("2026-09-03")
    called = _stub_quote(monkeypatch, {"KBS": ["2026-09-04"], "VCI": ["2026-09-04"]})
    assert main._ohlc_last_date(main._get_equity_ohlc("HCM", 260)) == "2026-09-04"
    assert called == ["KBS"]


def test_chuoi_ngan_duoi_200_van_bi_bo_qua(monkeypatch):
    """Ngưỡng độ dài cũ KHÔNG bị bản vá làm mất."""
    _seed_vnindex("2026-09-03")

    class FakeQuote:
        def __init__(self, symbol, source):
            self.source = source

        def history(self, **kwargs):
            if self.source == "KBS":
                return pd.DataFrame({"time": ["2026-09-03"], "close": [10.0]})
            return _df(["2026-09-03"])

    monkeypatch.setattr(main, "Quote", FakeQuote)
    assert main._ohlc_last_date(main._get_equity_ohlc("HCM", 260)) == "2026-09-03"


# ── mốc TỰ ẤM từ lát cắt ngang (cache VN-Index thường nguội) ───────────────────────────────────
# [ĐO] 04/09/2026: `GET /api/vnindex-overview` trên prod trả HTTP 500 sau 70s, và `/api/history`
# chỉ chạm VN-Index khi client hỏi đúng symbol VNINDEX (hiếm — client lấy index từ DNSE).
# Không có cơ chế tự ấm thì phép kiểm tra độ tươi gần như luôn nằm im.

def test_moc_tu_am_khi_KHONG_co_cache_vnindex(monkeypatch):
    """Hai mã đầu dạy cho service biết 03/09 tồn tại; mã thứ ba trễ thì bị bắt."""
    _stub_quote(monkeypatch, {"KBS": ["2026-09-03"], "VCI": ["2026-09-03"]})
    main._get_equity_ohlc("FPT", 260)
    assert main._latest_known_session_date() is None, "một mã thì chưa đủ phiếu"
    main._get_equity_ohlc("VNM", 260)
    assert main._latest_known_session_date() == "2026-09-03"

    called = _stub_quote(monkeypatch, {"KBS": ["2026-08-28"], "VCI": ["2026-09-03"]})
    out = main._get_equity_ohlc("BAF", 260)
    assert main._ohlc_last_date(out) == "2026-09-03"
    assert called == ["KBS", "VCI"], "phải thử tiếp vì mốc nay đã biết 03/09"


def test_ngay_TUONG_LAI_khong_duoc_ghi_nhan():
    main._record_observed_session_date("AAA", "2099-01-01")
    main._record_observed_session_date("BBB", "2099-01-01")
    assert main._latest_known_session_date() is None


def test_mot_ma_lap_lai_khong_tu_nang_phieu():
    main._record_observed_session_date("AAA", "2026-09-03")
    main._record_observed_session_date("AAA", "2026-09-03")
    assert main._latest_known_session_date() is None


def test_khong_phinh_bo_nho():
    for i in range(200):
        d = f"2026-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
        main._record_observed_session_date(f"S{i}", d)
    assert len(main._observed_session_dates) <= main._OBSERVED_KEEP_DATES


# ── danh sách nguồn ────────────────────────────────────────────────────────────────────────────

def test_chi_con_hai_nguon_Quote_hop_le():
    assert main._VALID_QUOTE_SOURCES == ("KBS", "VCI")


def test_khong_con_TCBS_DNSE_trong_duong_gia(monkeypatch):
    """TCBS/DNSE ném ValueError trên vnstock 4.0.7 → gọi chúng là phí một vòng lặp."""
    _seed_vnindex("2026-09-03")
    called = _stub_quote(monkeypatch, {"KBS": ["2026-08-28"], "VCI": ["2026-08-28"]})
    main._get_equity_ohlc("HCM", 260)
    assert "TCBS" not in called and "DNSE" not in called
