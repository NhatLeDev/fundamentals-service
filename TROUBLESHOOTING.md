# Troubleshooting Guide - Fundamentals Service

## Issue: Money Flow Data Returns Null

### Symptoms
The `/api/moneyflow` endpoint returns `null` for all money flow fields:
```json
{
  "foreignBuy": null,
  "foreignSell": null,
  "proprietaryBuy": null,
  "proprietarySell": null
}
```

### Root Causes

#### 1. **vnstock-data Package Issues**
The money flow data requires `vnstock-data-python` package which is still in development and may have:
- Breaking API changes
- Source provider (VCI/TCBS/SSI) API rate limits or maintenance
- Authentication issues with data providers

**Solution:**
```bash
# Verify package is installed
pip show vnstock-data

# Try reinstalling
pip install --upgrade git+https://github.com/vuthanhdatt/vnstock-data-python.git
```

#### 2. **Data Source Limitations**
Vietnamese stock market data providers may:
- Block server IPs (especially cloud providers)
- Rate limit requests aggressively
- Require specific User-Agent headers
- Only work during market hours (9:00-15:00 VN time)

**Solution:**
The service now tries multiple sources in order: VCI → TCBS → SSI

#### 3. **No Trading Activity**
Some stocks may have no foreign or proprietary trading in the requested period.

**Solution:**
Try liquid stocks like VNM, VIC, HPG with shorter `days` parameter.

### Diagnostic Steps

#### Step 1: Test Health Check
```bash
curl https://fundamentals-service.onrender.com/api/health
```

Expected response:
```json
{
  "status": "ok",
  "trading_available": true,
  "vnstock_available": true
}
```

If `trading_available: false`, the vnstock-data package is not installed.

#### Step 2: Check Debug Response
The `/api/moneyflow` endpoint includes debug information:
```json
{
  "data": { ... },
  "_debug": {
    "requested_tickers": ["SSI"],
    "days": 30,
    "trading_available": true
  }
}
```

#### Step 3: Test Locally
```bash
cd fundamentals-service
pip install -r requirements.txt
uvicorn main:app --reload --port 8001

# In another terminal
curl -X POST http://localhost:8001/api/moneyflow \
  -H "Content-Type: application/json" \
  -d '{"tickers": ["VNM"], "days": 7}'
```

#### Step 4: Manual Package Test
Create `test_trading.py`:
```python
from vnstock_data import Trading
from datetime import date, timedelta

end_d = date.today()
start_d = end_d - timedelta(days=7)
start_str = start_d.strftime("%Y-%m-%d")
end_str = end_d.strftime("%Y-%m-%d")

# Test foreign trading
try:
    trading = Trading(symbol="VNM", source="vci")
    df = trading.foreign_trade(start=start_str, end=end_str)
    print("Foreign trade data:")
    print(df.head())
    print(f"\nColumns: {df.columns.tolist()}")
except Exception as e:
    print(f"Error: {e}")
```

Run: `python test_trading.py`

### Known Working Alternatives

If vnstock-data continues to have issues, consider:

1. **SSI API** (requires registration)
   - https://iboard-ssi.ssi.com.vn/dchart/api/foreign-trading

2. **TCBS API** (free but may have limits)
   - Uses internal endpoints, check network tab on https://banggia.tcbs.com.vn

3. **VCI API** (requires account)
   - Similar to SSI, more stable but requires authentication

### Workaround: Graceful Degradation

The Portfolio page now gracefully handles missing money flow data:
- Displays helpful message explaining why data might be unavailable
- AI analysis continues using price, volume, and technical indicators
- Money flow data is treated as optional enhancement, not requirement

### Production Checklist

- [ ] Verify `requirements.txt` includes vnstock-data
- [ ] Check Render deployment logs for package installation errors
- [ ] Test `/api/health` endpoint shows `trading_available: true`
- [ ] Monitor response times (should be < 10s per ticker)
- [ ] Consider caching results for 15-30 minutes
- [ ] Set up monitoring alerts for consistent null responses

### Contact & Support

- **vnstock**: https://github.com/thinh-vu/vnstock
- **vnstock-data**: https://github.com/vuthanhdatt/vnstock-data-python
- **Community**: Join vnstock Discord/Telegram for Vietnamese stock market data discussions
