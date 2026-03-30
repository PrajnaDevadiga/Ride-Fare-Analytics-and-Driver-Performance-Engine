## Ride Fare Analytics & Driver Performance Engine

Analyze ride data, validate records, detect anomalies, and compute driver performance KPIs.

### Features
- **Driver validation**: Only `status == ACTIVE` with non-missing `driver_id`
- **Ride validation**: Keep only rides with valid driver, `fare_amount > 0`, valid `ride_time`, and `ride_status == COMPLETED`
- **Anomaly detection**:
  - `HIGH_FARE`: fare_amount > 500
  - `RAPID_RIDES`: more than 2 rides within a 2-minute window per driver
- **Performance aggregation**: per-driver `total_rides`, `total_earnings`, `avg_fare`

### Project Structure
- `src/engine.py`: Core processing (validation, anomalies, aggregation)
- `src/main.py`: CLI entry point
- `tests/test_engine.py`: Unit tests
- `requirements.txt`: Python dependencies
- `.cursor/rules/project-structure.mdc`: Workspace navigation rule (Cursor)

### Requirements
- Python 3.10+
- Install dependencies:

```bash
pip install -r requirements.txt
```

### Input Files
Provide two CSVs: `drivers.csv` and `rides.csv`.

- `drivers.csv` required columns:
  - `driver_id` (string)
  - `status` (string, values include `ACTIVE`, `BLOCKED`)

- `rides.csv` required columns:
  - `driver_id` (string; must exist in active drivers)
  - `fare_amount` (number; must be > 0)
  - `ride_time` (ISO-8601 timestamp; e.g., `2024-01-01T10:00:00Z`)
  - `ride_status` (string; must be `COMPLETED` to be included)

Notes:
- Timestamps are parsed in UTC. Invalid timestamps are rejected.
- Any missing required column yields an empty validated set.

### Run the Pipeline
From the project root:

```bash
python -m src.main --drivers drivers.csv --rides rides.csv --outdir .
```

On Windows PowerShell:

```powershell
python -m src.main --drivers .\drivers.csv --rides .\rides.csv --outdir .
```

Outputs written to `--outdir`:
- `driver_performance.csv`:
  - Columns: `driver_id, total_rides, total_earnings, avg_fare`
- `anomaly_report.csv`:
  - Columns: `driver_id, ride_time, fare_amount, anomaly_type`

### Anomaly Definitions
- **HIGH_FARE**: `fare_amount > 500`
- **RAPID_RIDES**: For a given `driver_id`, any rides that fall within a sliding 2-minute window where the count is ≥ 3 are flagged.

### Developer Usage (Python API)
You can import and reuse the engine in Python:

```python
from src.engine import run_pipeline

performance_df, anomalies_df = run_pipeline("drivers.csv", "rides.csv")
print(performance_df.head())
print(anomalies_df.head())
```

### Testing
Run the unit tests:

```bash
pytest -q
```

Included tests:
- `test_invalid_driver_rejected`
- `test_blocked_driver_rejected`
- `test_negative_fare_ignored`
- `test_high_fare_flag`
- `test_rapid_rides_flag`
- `test_driver_earnings_calculation`

### Example CSV Schemas
Minimal examples to illustrate headers (data is illustrative only):

```csv
# drivers.csv
driver_id,status
d1,ACTIVE
d2,BLOCKED
```

```csv
# rides.csv
driver_id,fare_amount,ride_time,ride_status
d1,100,2024-01-01T10:00:00Z,COMPLETED
d1,600,2024-01-01T12:00:00Z,COMPLETED
```

### Troubleshooting
- If outputs are empty, ensure required columns exist and values meet validation criteria.
- Confirm `ride_time` values are valid ISO timestamps (e.g., include `Z` for UTC).
