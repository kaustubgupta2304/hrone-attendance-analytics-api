# Employee Attendance & Analytics API

FastAPI + MongoDB (6.0+). All code is in `app/main.py`.

## Run
```bash
pip install -r requirements.txt

# macOS / Linux
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"
# Windows CMD
#   set MONGO_URI=mongodb://localhost:27017
#   set MONGO_DB=attendance_db

uvicorn app.main:app --port 8000
```

You can also put these two values in a local `.env` file.
Indexes are created at startup (idempotent). Real environment variables take priority over `.env`.

## Notes and assumptions
- **Trend gap filling** uses `$facet` + `$range` + `$map` instead of `$densify`, so empty ranges (no records at all) still produce one row per day on any 6.0+ server. Everything, including the 7-day window (`$setWindowFields`), runs in the database.
- **`moving_avg_7d`** averages the *reported* (already 4-decimal, half-up) `attendance_rate` values.
- **Rounding** is always half-up and exact (`Decimal` or integer arithmetic). MongoDB's `$round` rounds half to even, so it is not used for reported numbers.
- **Weekday** of a record is taken from its `date` string. `working_days` for the monthly endpoint is calendar arithmetic in Python; the data itself is aggregated in MongoDB.
- `PATCH` retries up to 5 times on a concurrent write (optimistic check on the history length), then returns 409.

## Known limits
The department summary, late leaderboard and department trend use `$setWindowFields`, `$facet`/`$range` and `$lookup` with a sub-pipeline. They need MongoDB 6.0+.
