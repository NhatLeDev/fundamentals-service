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

## 3. Biến môi trường (tuỳ chọn)

| Biến | Ý nghĩa |
|------|--------|
| `VNSTOCK_API_KEY` | API key từ [vnstocks.com/login](https://vnstocks.com/login) – tăng giới hạn (60 req/phút Community). Không set thì dùng chế độ Guest (20 req/phút). |
| `VNSTOCK_SOURCE` | Nguồn dữ liệu: `KBS` (mặc định) hoặc `VCI`. |

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
    "SSI": { "pe": 7.19, "pb": 1.02, "roe": 18.5, "eps": 3500 },
    "MBB": { "pe": 6.85, "pb": 0.95, "roe": 17.2, "eps": 3200 }
  }
}
```

- Mã không lấy được sẽ không có trong `data`.
- Dữ liệu lấy từ `vnstock`: `Finance(symbol, source=...).ratio(period='year')` và `Company(...).overview()`.

## 5. Tích hợp Next.js

Trong `.env` của project Next.js:

```bash
FUNDAMENTALS_API_URL=http://localhost:8001/fundamentals
```

Khi có biến này, `fetchFundamentalsForTickers` trong `market-api.ts` sẽ gọi sang service và đưa dữ liệu vào prompt Gemini.
