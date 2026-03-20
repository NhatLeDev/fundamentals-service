# VN Fundamentals Service (vnstock)

Backend Python dùng [vnstock](https://github.com/thinh-vu/vnstock) để lấy P/E, P/B, ROE, EPS cho cổ phiếu Việt Nam, expose REST API cho app Next.js (và gửi vào prompt AI).

## 1. Cài đặt

- Python 3.10+
- Trong thư mục `fundamentals-service/`:

```bash
pip install -r requirements.txt
```

## 2. Chạy

```bash
cd fundamentals-service
uvicorn main:app --reload --port 8001
```

Service lắng nghe tại `http://localhost:8001`.

## 3. Biến môi trường

### Chạy local (repo riêng hoặc trong monorepo)

Tạo file `.env` từ mẫu (không commit `.env`):

```bash
cp .env.example .env
# Sửa .env: bỏ comment và điền VNSTOCK_API_KEY nếu có
```

### Deploy (Render / Railway / Cloud Run)

**Không cần** file `.env` trên server. Cấu hình trực tiếp trên dashboard:

| Biến              | Ý nghĩa                                                                                                                                      |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `VNSTOCK_API_KEY` | API key từ [vnstocks.com/login](https://vnstocks.com/login) – tăng giới hạn (60 req/phút Community). Không set thì dùng Guest (20 req/phút). |
| `VNSTOCK_SOURCE`  | Nguồn dữ liệu: `KBS` (mặc định) hoặc `VCI`.                                                                                                  |
| `PORT`            | Render/Railway tự gán; không cần set thủ công.                                                                                               |

## 4. API

### POST `/fundamentals`

**Request:**

```json
{
  "tickers": ["SSI", "MBB", "HDB", "TCB"]
}
```

**Response:**

```json
{
  "data": {
    "SSI": {
      "pe": 7.19,
      "pb": 1.02,
      "roe": 18.5,
      "eps": 3500,
      "volume_ma20": 1245000.25,
      "volume_ma50": 1189000.5,
      "cash_flow_operating": 1234567890000,
      "cash_flow_net": 500000000000,
      "trading_flow": {
        "foreign_net_value": 10000000000,
        "foreign_net_volume": 500000,
        "proprietary_net_value": -2000000000,
        "proprietary_net_volume": -100000
      }
    },
    "MBB": { "pe": 6.85, "pb": 0.95, "roe": 17.2, "eps": 3200 }
  }
}
```

- Mã không lấy được sẽ không có trong `data`.
- **Volume MA:** `volume_ma20`, `volume_ma50` được tính từ lịch sử khối lượng giao dịch ngày (`Quote.history(..., interval="1D")`).
- **Dòng tiền (cash flow):** `cash_flow_operating` (lưu chuyển tiền từ hoạt động kinh doanh), `cash_flow_net` (tăng/giảm tiền thuần) từ `Finance(...).cash_flow(period='year')` (KBS/VCI). Có thể thiếu nếu nguồn không trả về.
- **Khối ngoại & tự doanh:** `trading_flow` chỉ xuất hiện nếu cài thêm [vnstock-data](https://github.com/vuthanhdatt/vnstock-data-python) (`pip install git+https://github.com/vuthanhdatt/vnstock-data-python.git`). Gồm `foreign_net_value`, `foreign_net_volume`, `proprietary_net_value`, `proprietary_net_volume` (tổng 30 ngày gần nhất). Không cài thì phần này đánh giá ở mức tổng quát.
- Dữ liệu cơ bản lấy từ `vnstock`: `Finance(symbol, source=...).ratio(period='year')`, `Finance(...).cash_flow(period='year')` và `Company(...).overview()`.

### POST `/api/moneyflow`

Endpoint frontend của bạn đang gọi để lấy dữ liệu **khối ngoại & tự doanh** theo dạng mua/bán.

**Request:**

```json
{
  "tickers": ["SSI", "MBB"],
  "days": 30
}
```

**Response:**

```json
{
  "data": {
    "SSI": {
      "foreignBuy": 15000000000,
      "foreignSell": 12000000000,
      "proprietaryBuy": 5000000000,
      "proprietarySell": 4500000000,
      "foreignRoomCurrent": 1000000,
      "foreignRoomTotal": 5000000,
      "foreignOwnership": 20.5
    }
  },
  "_debug": {
    "requested_tickers": ["SSI", "MBB"],
    "days": 30,
    "trading_available": true
  }
}
```

**Ghi chú quan trọng:**
- Dữ liệu khối ngoại/tự doanh cần cài [vnstock-data](https://github.com/vuthanhdatt/vnstock-data-python) (`pip install git+https://github.com/vuthanhdatt/vnstock-data-python.git`).
- `foreignBuy/foreignSell` là **giá trị mua/bán** tổng theo cửa sổ `days` (tính trong `Trading.foreign_trade()`), tương tự cho `proprietaryBuy/proprietarySell` (tính trong `Trading.prop_trade()`).
- **Nếu trả về `null`**: Có thể do:
  - Nguồn dữ liệu (VCI/TCBS/SSI) đang bảo trì hoặc giới hạn request
  - Mã cổ phiếu không có giao dịch trong kỳ
  - Package `vnstock-data` chưa được cài đặt đúng cách
- **Giải pháp**: Dữ liệu này là tùy chọn. AI vẫn phân tích được dựa trên giá, khối lượng và các chỉ báo kỹ thuật khác.

### GET `/health` hoặc `/api/health`

Endpoint kiểm tra trạng thái service.

**Response:**

```json
{
  "status": "ok",
  "timestamp": "2026-03-20T10:30:00.123456",
  "service": "fundamentals-api",
  "version": "1.0.0",
  "vnstock_available": true,
  "trading_available": true
}
```

### GET `/api/vnindex-overview`

Trả về tổng quan VN-Index: giá đóng cửa gần nhất và MA(20/50/200) để đánh giá xu hướng thị trường.

**Response:**

```json
{
  "last": 1268.45,
  "ma20": 1255.32,
  "ma50": 1240.18,
  "ma200": 1180.5
}
```

- Dùng cho báo cáo phân tích khi cần nhận định xu hướng thị trường chung.
- `fetchVnIndexOverview()` trong `market-api.ts` gọi endpoint này khi có `FUNDAMENTALS_API_URL` hoặc `VNINDEX_OVERVIEW_API_URL`.

## 5. Tích hợp Next.js

Trong `.env` của project Next.js:

```bash
FUNDAMENTALS_API_URL=http://localhost:8001
```

Lưu ý:
- Nếu frontend ghép path kiểu `FUNDAMENTALS_API_URL + /api/moneyflow` thì biến phải là **base URL** như trên (không thêm `/fundamentals`).
- Service hỗ trợ cả:
  - `POST /api/fundamentals` (khuyến nghị)
  - `POST /fundamentals`
  - `POST /api/moneyflow` (khối ngoại/tự doanh + room ngoại)
