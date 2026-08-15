# SPX Option Chain Microservice

A fast, lightweight, and **free-tier compatible** API deployed on **Google Cloud Run** or **Cloud Functions (2nd Gen)**.

It queries Cboe's 15-minute delayed market quotes, caches the entire chain in memory (60s TTL), and exposes clean endpoints to filter strikes, expirations, option types, and calculate Net Gamma Exposure (GEX).

---

## Endpoints

### 1. `GET /spx/metadata`
Returns current underlying index price (`^SPX`), timestamp, and all available expiration dates (0DTE, daily weeklies, and monthly options).

### 2. `GET /spx/chain`
Returns the option chain for a given expiration date, filtered by strikes or option types.

**Query Parameters:**
- `expiration` *(optional)*: `YYYY-MM-DD` (defaults to nearest active expiration).
- `type` *(optional)*: `CALL` or `PUT`.
- `strike_min` *(optional)*: Minimum strike.
- `strike_max` *(optional)*: Maximum strike.
- `strike_range` *(optional)*: Strike window centered on current spot price (e.g. `strike_range=50` gets `spot - 50` to `spot + 50`).

### 3. `GET /spx/atm`
Quick helper returning near-the-money options around spot.
- `points` *(optional, default: 50.0)*: Window around spot price.

### 4. `GET /spx/gex`
Computes **Net Gamma Exposure (GEX)** per strike in millions of dollars ($M) for visualizing market maker gamma walls.
- `expiration` *(optional)*: Target expiration date.
- `strike_range` *(optional, default: 150.0)*: Strike window for GEX profile.

---

## Local Development & Testing

1. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```
2. Start the local server:
   ```bash
   uvicorn main:app --reload --port 8080
   ```
3. Open interactive Swagger documentation in your browser:
   ```text
   http://localhost:8080/docs
   ```

---

## Deployment to Google Cloud Run (Free Tier)

Run either deployment script:

### PowerShell:
```powershell
.\deploy.ps1
```

### Bash:
```bash
./deploy.sh
```

Or run directly with `gcloud`:
```bash
gcloud run deploy spx-options-api \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --concurrency 80 \
  --min-instances 0 \
  --max-instances 2
```
