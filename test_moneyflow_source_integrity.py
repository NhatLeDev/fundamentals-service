"""
QC ĐỘC LẬP (agent `qc`) — test ĐỎ cho hai đường rò còn lại sau bản vá "tự doanh bịa".

Hai test dưới đây KHÔNG phải đề xuất thiết kế: chúng kiểm đúng BẤT BIẾN mà chính bản vá tự
tuyên bố trong `main.py`:
  - "Giữ None xuyên suốt để cờ đó tắt" (docstring `_vnstock_moneyflow_to_api_shape`)
  - "Thà THIẾU còn hơn SAI" / "Vì sao phải trả None chứ không phải 0"
    (khối ghi chú TỰ DOANH trong `_compute_money_flow_for_symbol`)

Chạy:
  cd fundamentals-service && env -u VNSTOCK_API_KEY VNSTOCK_DISABLE_AGENT_SETUP=1 \
      <python> -m pytest -q test_qc_moneyflow_leaks.py
"""
from __future__ import annotations

import datetime

import main


# ─────────────────────────────────────────────────────────────────────────────────────────────
# Bqc-1 — AC-A ("không còn đường nào khiến TỰ DOANH ra SỐ, kể cả 0, tới consumer")
#
# Đường rò: `_get_moneyflow` (main.py:721-726) chỉ ghi khi vế hiện tại còn None
#   `if v is not None and (merged.get(k) is None): merged[k] = v`
# ⇒ một `0.0` từ nguồn ĐẦU TIÊN khoá vĩnh viễn vế đó, nguồn sau có số thật cũng không ghi đè được.
# Chốt "pb và ps đều None-hoặc-0 ⇒ ép None" (main.py:766-767) KHÔNG bắt được ca lệch vế:
# pb = 0.0 (bịa, do khoá) + ps = số thật ⇒ cả cặp đi tiếp.
# Hậu quả: market-api.ts:4427-4429 bật `proprietaryAvailable` (pb là số hữu hạn) ⇒
# gemini.ts:1857 in "Tự doanh: Mua 0 · Bán 500.000.000 · Ròng -500.000.000" NHƯ SỰ THẬT.
# ─────────────────────────────────────────────────────────────────────────────────────────────
class _FakeTrading:
    """Giả `vnstock_data.Trading`. Trả dict (không phải DataFrame) để đi nhánh `isinstance(dict)`."""

    def __init__(self, symbol: str, source: str):
        self.source = source

    def foreign_trade(self, start=None, end=None):
        return None

    def prop_trade(self, start=None, end=None, resolution=None):
        if self.source == "vci":
            # Nguồn đầu CÓ cột mua nhưng tổng = 0 (không có lệnh tự doanh nào trong cửa sổ,
            # hoặc cột rỗng được sum thành 0.0) — KHÔNG có cột bán.
            return {"total_buy_trade_value": 0.0}
        # Nguồn thứ hai mới có số BÁN thật.
        return {"total_sell_trade_value": 5e8}


def test_AC_A_Bqc_1_so_0_bi_khoa_tu_nguon_dau_van_ra_toi_consumer(monkeypatch):
    monkeypatch.setattr(main, "_Trading", _FakeTrading)

    flat = main._get_moneyflow("ACB", days=30)
    # Chụp lại hiện trạng để khi đỏ thì đọc được ngay nguyên nhân.
    assert flat is not None
    assert flat["proprietarySell"] == 5e8, "nguồn 2 phải vào được vế bán"

    out = main._vnstock_moneyflow_to_api_shape(flat, total_days=30)
    assert out is not None

    # BẤT BIẾN BỊ VI PHẠM: `proprietaryBuy` ở đây là 0 BỊA (nguồn đầu khoá), không phải "tự doanh
    # mua 0". Nó là SỐ ⇒ `proprietaryAvailable` bật ⇒ prompt in như sự thật.
    assert out["proprietaryBuy"] is None, (
        "0.0 bị KHOÁ ở _get_moneyflow (main.py:724) lọt ra consumer dưới dạng SỐ: "
        f"proprietaryBuy={out['proprietaryBuy']!r}, proprietarySell={out['proprietarySell']!r} "
        "⇒ market-api.ts:4427-4429 bật proprietaryAvailable ⇒ gemini.ts:1857 in "
        "'Tự doanh: Mua 0 · Bán 500.000.000' NHƯ SỰ THẬT"
    )


def test_AC_A_Bqc_1b_di_qua_ca_build_moneyflow_response(monkeypatch):
    """Cùng lỗi, nhưng đi hết đường tới payload mà endpoint `/api/moneyflow` trả ra."""
    monkeypatch.setattr(main, "_Trading", _FakeTrading)
    # Không có token SSI ⇒ đúng hiện trạng production (`ssi_token_available: false`).
    monkeypatch.setattr(main, "_ssi_get_access_token", lambda **kwargs: None)
    main._moneyflow_cache.clear()

    out, debug = main._build_moneyflow_response(["ACB"], 30, 20)
    assert debug["ssi_token_available"] is False
    payload = out.get("ACB")
    assert payload is not None

    assert payload["proprietaryBuy"] is None, (
        "payload của /api/moneyflow mang proprietaryBuy=0 BỊA: "
        f"{ {k: payload[k] for k in ('foreignBuy', 'proprietaryBuy', 'proprietarySell')} }"
    )


# ─────────────────────────────────────────────────────────────────────────────────────────────
# Bqc-2 — AC-B ("khối ngoại giờ có thể là None mà không phá consumer nào")
#
# Đường rò NẰM TRONG CHÍNH HÀM VỪA VIẾT LẠI: main.py:2746-2748
#   totals = [p for p in parsed if p["d"] >= start_total]
#   foreignBuy = round(sum(p["foreignBuy"] for p in totals))
# `sum([])` = 0 trong Python ⇒ cửa sổ `total_days` KHÔNG có phiên nào vẫn ra `foreignBuy: 0`,
# `foreignSell: 0` — y hệt lớp lỗi `or 0` mà bản vá vừa diệt ở vế tự doanh, chỉ khác là ở vế
# KHỐI NGOẠI thì không ai vá. Và vế khối ngoại KHÔNG có cờ `foreignAvailable` nào cả
# (market-api.ts:4410-4417 lấp thẳng 0), nên gemini.ts:1845 in VÔ ĐIỀU KIỆN
# "Khối ngoại: Mua 0 · Bán 0 · Ròng +0" NHƯ SỰ THẬT.
#
# Kịch bản thật: mã ngừng giao dịch / bị đình chỉ / nghỉ dài hơn cửa sổ `days`, hoặc client gọi
# `days` nhỏ (1–3) vào kỳ nghỉ lễ. `parsed` vẫn có phiên (cửa sổ trend rộng hơn: max(60, trend*2)
# ngày) nên hàm KHÔNG trả None, mà trả 0.
# ─────────────────────────────────────────────────────────────────────────────────────────────
def _items_older_than_totals_window(n: int = 5):
    """Phiên THẬT nhưng đều nằm NGOÀI cửa sổ `total_days` (trong cửa sổ trend)."""
    today = datetime.date.today()
    out = []
    for i in range(n):
        d = today - datetime.timedelta(days=40 + i)  # < 60 ngày (trend) nhưng > 30 ngày (totals)
        out.append(
            {
                "TradingDate": d.strftime("%d/%m/%Y"),
                "ForeignBuyValTotal": "1000",
                "ForeignSellValTotal": "400",
            }
        )
    return out


def test_AC_B_Bqc_2_cua_so_totals_rong_sinh_ra_khoi_ngoai_0_bia(monkeypatch):
    monkeypatch.setattr(
        main, "_ssi_get_daily_stock_price", lambda **kwargs: _items_older_than_totals_window()
    )
    res = main._compute_money_flow_for_symbol(
        symbol="ACB",
        total_days=30,
        trend_sessions=20,
        ssi_base_url="https://example.invalid",
        token="fake-token",
    )
    assert res is not None, "vẫn có chuỗi trend nên hàm không trả None"
    assert res["foreignNetSeries"], "chuỗi trend vẫn có phiên thật"

    # KHÔNG có quan sát nào trong cửa sổ tổng ⇒ 0 ở đây là BỊA, không phải 'khối ngoại không mua'.
    assert res["foreignBuy"] is None, (
        "sum([]) = 0 ⇒ foreignBuy/foreignSell = 0 dù KHÔNG có phiên nào trong cửa sổ total_days: "
        f"foreignBuy={res['foreignBuy']!r}, foreignSell={res['foreignSell']!r} "
        "⇒ market-api.ts:4410-4417 giữ nguyên 0 ⇒ gemini.ts:1845 in "
        "'Khối ngoại: Mua 0 · Bán 0 · Ròng +0' NHƯ SỰ THẬT (không có cờ nào gate vế này)"
    )
