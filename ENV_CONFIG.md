# Environment Configuration for Rate Limit Fix

## Required Environment Variables

Add these to your deployment environment (Render, Vercel, etc.) or `.env` file:

### Rate Limiting
```bash
# Maximum vnstock API calls per minute (default: 10, which is 50% of Guest tier)
# With registered API key, you can increase this to 30-40 (Community tier = 60/min)
VNSTOCK_MAX_CALLS_PER_MINUTE=10
```

### VN-Index: nguồn trong nước (HOSE) vs Yahoo

```bash
# Mặc định trong code = 1: SSI FastConnect → vnstock → Yahoo → Robotstock.
# Đặt 0|false nếu môi trường chỉ dùng được Yahoo (không có SSI_FC_* và không muốn gọi vnstock trước).
VNINDEX_PREFER_DOMESTIC=1
```

### Cache TTLs (Time-to-Live in seconds)
```bash
# VN-Index overview cache (5 minutes = 300 seconds)
VNINDEX_CACHE_TTL_SECONDS=300

# VN30 breadth calculation cache (10 minutes = 600 seconds)  
# This is the CRITICAL one - caches 30+ API calls
VN30_BREADTH_CACHE_TTL_SECONDS=600

# Fundamentals data cache (5 minutes = 300 seconds)
FUNDAMENTALS_CACHE_TTL_SECONDS=300

# Money flow data cache (2 minutes = 120 seconds)
MONEYFLOW_CACHE_TTL_SECONDS=120
```

### vnstock API Key (IMPORTANT)
```bash
# Register FREE at: https://vnstocks.com/login
# Increases limit from 20 → 60 requests/minute
VNSTOCK_API_KEY=your_api_key_here
```

## Full .env Example

```bash
# ===========================================
# VNSTOCK CONFIGURATION
# ===========================================

# API Key (register at https://vnstocks.com/login)
# Guest tier: 20 req/min, Community tier: 60 req/min
VNSTOCK_API_KEY=vnstock_592ce82d749b63e34a0845bf0456f981

# Data source preference
VNSTOCK_SOURCE=KBS

# ===========================================
# RATE LIMITING
# ===========================================

# Max calls per minute to vnstock API
# Set to 50% of your tier limit for safety:
# - Guest (20/min) → set to 10
# - Community (60/min) → set to 30
VNSTOCK_MAX_CALLS_PER_MINUTE=10

# ===========================================
# CACHE TTLs (seconds)
# ===========================================

# VN-Index overview: refreshed every 5 minutes
VNINDEX_CACHE_TTL_SECONDS=300

# VN30 breadth: refreshed every 10 minutes (CRITICAL - saves 30+ calls)
VN30_BREADTH_CACHE_TTL_SECONDS=600

# Fundamentals (PE, PB, ROE, EPS): refreshed every 5 minutes
FUNDAMENTALS_CACHE_TTL_SECONDS=300

# Money flow: refreshed every 2 minutes
MONEYFLOW_CACHE_TTL_SECONDS=120

# ===========================================
# SERVER
# ===========================================

# Port (auto-set by Render/Railway)
PORT=8001
```

## Deployment Instructions

### For Render.com

1. Go to your service dashboard
2. Click "Environment" tab
3. Add each variable above as a new environment variable
4. Click "Save Changes"
5. Service will auto-redeploy

### For Vercel

1. Go to Project Settings → Environment Variables
2. Add each variable for Production, Preview, and Development
3. Redeploy the project

### For Railway

1. Go to your service
2. Click "Variables" tab
3. Add each variable
4. Service will auto-redeploy

## Testing Configuration

### Test 1: Verify API Key is Working

```bash
# Should show API key is registered
curl https://your-service.onrender.com/api/health | jq
```

Look for:
```json
{
  "vnstock_available": true,
  "rate_limiter": {
    "calls_in_last_minute": 0,
    "is_rate_limited": false,
    "max_calls_per_minute": 10
  }
}
```

### Test 2: Check Cache is Working

```bash
# First request (cold cache)
time curl https://your-service.onrender.com/api/vnindex-overview

# Second request (should be fast - cached)
time curl https://your-service.onrender.com/api/vnindex-overview
```

Second request should be < 200ms.

### Test 3: Monitor Rate Limiting

```bash
# Check rate limiter status
watch -n 5 'curl -s https://your-service.onrender.com/api/health | jq .rate_limiter'
```

Should see `calls_in_last_minute` stay below your `VNSTOCK_MAX_CALLS_PER_MINUTE` setting.

## Tuning Recommendations

### If Still Getting Rate Limited

1. **Increase Cache TTLs**:
   ```bash
   VNINDEX_CACHE_TTL_SECONDS=600    # 10 minutes
   VN30_BREADTH_CACHE_TTL_SECONDS=1800  # 30 minutes
   ```

2. **Decrease Rate Limit** (more conservative):
   ```bash
   VNSTOCK_MAX_CALLS_PER_MINUTE=5
   ```

3. **Register for Community Tier** (if haven't already):
   - Go to https://vnstocks.com/login
   - Register free account
   - Get API key
   - Increases limit to 60/min

### If Data Too Stale

1. **Decrease Cache TTLs**:
   ```bash
   VNINDEX_CACHE_TTL_SECONDS=180   # 3 minutes
   VN30_BREADTH_CACHE_TTL_SECONDS=300  # 5 minutes
   ```

2. **But keep VN30 breadth cache high** - it doesn't change frequently and costs 30+ API calls

### Optimal Settings for Different Tiers

#### Guest Tier (20 req/min)
```bash
VNSTOCK_MAX_CALLS_PER_MINUTE=8
VNINDEX_CACHE_TTL_SECONDS=300
VN30_BREADTH_CACHE_TTL_SECONDS=900  # 15 minutes
```

#### Community Tier (60 req/min)
```bash
VNSTOCK_MAX_CALLS_PER_MINUTE=30
VNINDEX_CACHE_TTL_SECONDS=180
VN30_BREADTH_CACHE_TTL_SECONDS=600
```

#### Sponsor Tier (180-600 req/min)
```bash
VNSTOCK_MAX_CALLS_PER_MINUTE=100
VNINDEX_CACHE_TTL_SECONDS=120
VN30_BREADTH_CACHE_TTL_SECONDS=300
```

## Monitoring

After deployment, monitor these metrics:

1. **Error Rate**: Should drop to near 0%
2. **Response Time**: 
   - Cold cache: 2-3s
   - Warm cache: <200ms
3. **Cache Hit Rate**: Should be >80%
4. **Rate Limiter Calls**: Should stay below limit

Check health endpoint regularly:
```bash
curl https://your-service.onrender.com/api/health
```
