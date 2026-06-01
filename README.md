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

### Chống timeout & rate limit (vnstock)

Mọi call vnstock đều đi qua 1 rate limiter dùng chung + có ngân sách thời gian phía
server, nên service luôn trả lời (kèm dữ liệu cache cũ nếu cần) thay vì để frontend
timeout. Các biến điều chỉnh:

| Biến                                | Mặc định | Ý nghĩa                                                                                              |
| ----------------------------------- | -------- | ---------------------------------------------------------------------------------------------------- |
| `VNSTOCK_MAX_CALLS_PER_MINUTE`      | `30`     | Trần gọi upstream/phút. Có `VNSTOCK_API_KEY` (Community 60) → 30 an toàn. Guest (không key) → hạ 10. |
| `FUNDAMENTALS_CACHE_TTL_SECONDS`    | `21600`  | TTL cache fundamentals (6h). PE/PB/ROE/EPS đổi theo quý nên cache dài để bớt gọi upstream.            |
| `FUNDAMENTALS_TOTAL_BUDGET_SECONDS` | `18`     | Tổng thời gian server dành cho 1 request fundamentals. **Phải < abort 25s của frontend.**            |
| `FUNDAMENTALS_PER_TICKER_TIMEOUT`   | `8`      | Timeout tối đa cho 1 mã trước khi rơi về cache cũ.                                                    |
| `FUNDAMENTALS_FETCH_WORKERS`        | `4`      | Số mã fetch song song (đã được rate limiter gate nên không gây 429).                                  |
| `MONEYFLOW_FUTURE_TIMEOUT`          | `18`     | Timeout mỗi mã cho luồng moneyflow.                                                                   |

Hành vi khi quá tải/lỗi: nếu một mã timeout / bị rate limit / nguồn trả rỗng mà còn
cache cũ → trả cache cũ kèm cờ `_stale: true`; worker vẫn chạy nền và tự làm ấm cache
cho request kế tiếp (stale-while-revalidate).

**Cold-start (Render free-tier):** instance ngủ sau ~15 phút idle → request đầu chờ
~30–50s (vượt abort 25s) và cache rỗng. Khắc phục: ping `GET /health` định kỳ (~10
phút) bằng cron ngoài (UptimeRobot / cron-job.org / GitHub Actions schedule) để giữ
ấm, hoặc rời free-tier.

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
