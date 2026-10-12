"""
Chặn hồi quy cho lỗi "tự doanh bịa": nguồn KHÔNG có tự doanh thì phải trả None, không trả số.

Chuỗi hậu quả (mỗi bước đọc được trong code, dẫn nguồn tại chỗ):
  1. `_compute_money_flow_for_symbol` từng tính `proprietary = max(0, TotalBuyTrade - ForeignBuyValTotal)`.
     Sai về ĐỊNH NGHĨA: hiệu đó là toàn bộ phần không phải khối ngoại — cá nhân + tổ chức trong nước
     + tự doanh CTCK gộp lại — chứ không phải riêng tự doanh. (Tỷ lệ từng khối: [GIẢ ĐỊNH], chưa đo.)
  2. `_vnstock_moneyflow_to_api_shape` từng dùng `or 0`, biến "nguồn không trả" thành 0.
  3. Consumer bật cờ `proprietaryAvailable` khi thấy SỐ HỮU HẠN — 0 cũng là số hữu hạn
     (stock-analysis/app/lib/market-api.ts:3948-3950).
  4. `gemini.ts:1857` in "Tự doanh: Mua X · Bán Y · Ròng Z" NHƯ SỰ THẬT vào prompt ra khuyến nghị.

Nên bất biến phải giữ: KHÔNG CÓ NGUỒN ⇒ None xuyên suốt, để prompt in "KHÔNG CÓ DỮ LIỆU".
"""
from __future__ import annotations

import datetime

import main


def _fake_items(n: int = 5):
    """Phiên giả có đủ 4 trường mà bản cũ dùng để bịa tự doanh."""
    today = datetime.date.today()
    out = []
    for i in range(n):
        d = today - datetime.timedelta(days=i)
        out.append(
            {
                "TradingDate": d.strftime("%d/%m/%Y"),
                "ForeignBuyValTotal": "1000",
                "ForeignSellValTotal": "400",
                # Hai trường này là cái bản cũ trừ đi để ra "tự doanh".
                "TotalBuyTrade": "50000",
                "TotalSellTrade": "50000",
            }
        )
    return out


def test_ssi_path_khong_bia_tu_doanh_tu_total_tru_foreign(monkeypatch):
    monkeypatch.setattr(
        main, "_ssi_get_daily_stock_price", lambda **kwargs: _fake_items()
    )
    res = main._compute_money_flow_for_symbol(
        symbol="ACB",
        total_days=30,
        trend_sessions=20,
        ssi_base_url="https://example.invalid",
        token="fake-token",
    )
    assert res is not None, "phải trả dữ liệu khối ngoại, chỉ bỏ tự doanh"

    # Bất biến chính: tự doanh là None, KHÔNG phải 0 và KHÔNG phải total - foreign.
    assert res["proprietaryBuy"] is None
    assert res["proprietarySell"] is None
    assert res["proprietaryNetSeries"] is None

    # Giá trị cũ sai mà test này chặn: 50000 - 1000 = 49000 mỗi phiên.
    assert res["proprietaryBuy"] != 49000 * 5

    # Khối ngoại là số THẬT từ nguồn ⇒ không được đụng tới (chặn hồi quy chiều ngược lại).
    assert res["foreignBuy"] == 1000 * 5
    assert res["foreignSell"] == 400 * 5
    assert res["foreignNetSeries"] and len(res["foreignNetSeries"]) == 5
    assert res["foreignNetSeries"][0]["net"] == 600


def test_vnstock_shape_giu_none_khong_lap_0():
    """Nguồn vnstock trả khối ngoại nhưng không có tự doanh ⇒ tự doanh phải ở None."""
    out = main._vnstock_moneyflow_to_api_shape(
        {"foreignBuy": 1000.0, "foreignSell": 400.0,
         "proprietaryBuy": None, "proprietarySell": None},
        total_days=30,
    )
    assert out is not None
    assert out["foreignBuy"] == 1000
    assert out["proprietaryBuy"] is None, "None bị lấp thành 0 ⇒ prompt in 'Tự doanh: Mua 0' như sự thật"
    assert out["proprietarySell"] is None


def test_vnstock_shape_giu_so_tu_doanh_that():
    """Khi nguồn CÓ tự doanh thật thì phải giữ nguyên — không được chặn nhầm."""
    out = main._vnstock_moneyflow_to_api_shape(
        {"foreignBuy": None, "foreignSell": None,
         "proprietaryBuy": 777.0, "proprietarySell": 333.0},
        total_days=30,
    )
    assert out is not None
    assert out["proprietaryBuy"] == 777
    assert out["proprietarySell"] == 333
    assert out["foreignBuy"] is None


def test_vnstock_shape_rong_tra_none():
    assert main._vnstock_moneyflow_to_api_shape({}, total_days=30) is None
    assert (
        main._vnstock_moneyflow_to_api_shape(
            {"foreignBuy": None, "foreignSell": None,
             "proprietaryBuy": None, "proprietarySell": None},
            total_days=30,
        )
        is None
    )


def test_co_tu_doanh_that_thi_totals_nonzero_van_nhan_dien():
    """`_moneyflow_api_totals_nonzero` phải chịu được None mà không ném."""
    assert main._moneyflow_api_totals_nonzero(
        {"foreignBuy": 1, "foreignSell": 0, "proprietaryBuy": None, "proprietarySell": None}
    ) is True
    assert main._moneyflow_api_totals_nonzero(
        {"foreignBuy": None, "foreignSell": None, "proprietaryBuy": None, "proprietarySell": None}
    ) is False


def test_tu_doanh_0_voi_khoi_ngoai_that_VAN_phai_la_None():
    """Lỗ mà bản vá đầu BỎ SÓT — chốt 'tất cả None-hoặc-0' không bắt được ca này.

    Khối ngoại có số thật ⇒ payload đi tiếp; nếu tự doanh để 0 thì `proprietaryAvailable` BẬT và
    prompt in 'Tự doanh: Mua 0 · Bán 0' như sự thật. Đây CHÍNH LÀ lỗi cũ, chỉ đi đường khác.
    """
    out = main._vnstock_moneyflow_to_api_shape(
        {"foreignBuy": 1e9, "foreignSell": 8e8,
         "proprietaryBuy": 0.0, "proprietarySell": 0.0},
        total_days=30,
    )
    assert out is not None, "khối ngoại có số thật ⇒ vẫn phải trả payload"
    assert out["foreignBuy"] == 1_000_000_000
    assert out["proprietaryBuy"] is None
    assert out["proprietarySell"] is None


def test_tu_doanh_mot_ve_0_mot_ve_None_cung_ra_None():
    out = main._vnstock_moneyflow_to_api_shape(
        {"foreignBuy": 1e9, "foreignSell": 8e8,
         "proprietaryBuy": 0.0, "proprietarySell": None},
        total_days=30,
    )
    assert out["proprietaryBuy"] is None and out["proprietarySell"] is None


def test_tu_doanh_mot_ve_khac_0_thi_GIU_ca_cap():
    """Chỉ ép None khi CẢ HAI vế rỗng — một vế có số thật thì 0 ở vế kia là thông tin thật."""
    out = main._vnstock_moneyflow_to_api_shape(
        {"foreignBuy": None, "foreignSell": None,
         "proprietaryBuy": 777.0, "proprietarySell": 0.0},
        total_days=30,
    )
    assert out["proprietaryBuy"] == 777
    assert out["proprietarySell"] == 0


def test_bool_khong_duoc_coi_la_so():
    """`isinstance(True, int)` là True trong Python ⇒ không loại bool thì True thành 1."""
    assert main._vnstock_moneyflow_to_api_shape(
        {"foreignBuy": True, "foreignSell": None,
         "proprietaryBuy": None, "proprietarySell": None},
        total_days=30,
    ) is None


def test_NaN_khong_duoc_coi_la_so():
    assert main._vnstock_moneyflow_to_api_shape(
        {"foreignBuy": float("nan"), "foreignSell": float("nan"),
         "proprietaryBuy": float("nan"), "proprietarySell": float("nan")},
        total_days=30,
    ) is None
