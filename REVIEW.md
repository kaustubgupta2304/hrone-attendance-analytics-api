# REVIEW.md

Defects found in the starter `app/main.py`, in file order. "Fixed" refers to the current `app/main.py`.

| # | Where | What is wrong | How you'd notice it | How I fixed it |
|---|---|---|---|---|
| 1 | `compute_late_minutes` | Grace check is done *after* flooring: `minutes > 10`. 09:40:01 floors to 10, so it returns 0 (R2 says 10). | Shift 09:30, punch 09:40:01 -> 0 instead of 10. | Compare whole seconds: late if `secs > 600`, then `secs // 60`. |
| 2 | `compute_late_minutes` | Builds the shift start with `punch_in.replace(hour=..)` in the punch-in's *own* time zone, not IST. With UTC or server-local datetimes the "09:30" is the wrong instant. | Punch 10:05 IST stored as 04:35 UTC -> compared with 09:30 UTC -> huge or negative lateness. | Shift start is built as an IST datetime on the attendance date. |
| 3 | `compute_late_minutes` | Overnight shifts ignored (shift start belongs to the previous day); `int()` truncates toward zero instead of flooring; sub-second part not truncated (R1). | Shift 22:00-06:00, punch-in 21:55. Punch 09:40:00.900 treated as late. | Date comes from `attendance_date()` (R1); integer-second arithmetic; ms truncated at the boundary. |
| 4 | `compute_work_hours` | Python `round()` is float/banker's rounding, not half-up (R4). Also no truncation, and aware-minus-naive mixes raise `TypeError`. | 16182 s = 4.495 h gives 4.49 (should be 4.50, which also flips `half_day`). | `Decimal(secs)/3600` quantized `ROUND_HALF_UP`. |
| 5 | `compute_overtime` | `max(0, ...)` has no 30-minute minimum (R3); end time is always on the record date (wrong for overnight); naive/aware mismatch possible. | Punch-out 18:45 on a 18:30 shift gives 15 instead of 0. | Floor of seconds/60, kept only if >= 30; end is on the next day for overnight shifts. |
| 6 | (missing) | `half_day` (R5) is never computed anywhere. | Punch-out of a 3.5 h day leaves `half_day` false. | `derive_after_punch_out` sets it from the *rounded* work hours. |
| 7 | `EmployeeIn` | No validation: `emp_code` format, e-mail, `HH:MM` shapes, real calendar `joined_on`, lengths, `shift_start != shift_end`. | `joined_on: "2026-02-30"`, `shift_start: "25:99"` return 201. | Field patterns and validators; all give 422. |
| 8 | `PunchInIn` | `status` is a free string (accepts `ABSENT`, `LEAVE`, junk). `punched_at: Optional[int]` is lax: accepts floats, numeric strings, seconds-looking values; `if body.punched_at` treats 0 as "missing". | `{"status":"LEAVE"}` creates a LEAVE record with a punch-in; `"punched_at": 1783312500` (seconds). | `Literal` presence statuses; strict int in the ms range -> 422. |
| 9 | `MongoClient(...)` | Not `tz_aware`: every datetime read back is naive. | Arithmetic between a stored value and an aware value raises `TypeError` (500). | `tz_aware=True`; helpers also tolerate naive values. |
| 10 | `/health` | Always 200, never touches MongoDB. | Stop MongoDB: still "ok". | Pings MongoDB; 503 on failure. |
| 11 | `create_employee` | Check-then-insert race and no unique index, so concurrent creates make duplicates. `created_at` is `datetime.now()` (naive, server-local) and is returned as a datetime, not epoch ms. | Fire 20 identical POSTs in parallel. Compare `created_at` with the contract. | Unique index + `DuplicateKeyError` -> 409; `created_at` is UTC, returned as epoch ms. |
| 12 | `list_employees` | `skip = page * page_size` skips the first page; `total` counts the whole collection, not the filtered set; no sort; no bounds on `page`/`page_size` (0, negative, 10^6 -> negative skip -> 500). | `page=1` misses the first rows; `department=X` returns the wrong `total`. | `(page-1)*size`, filtered `count_documents`, sort by `emp_code`, `ge/le` constraints -> 422. |
| 13 | `punch_in` | Unknown employee: `emp` is `None`, then `emp["shift_start"]` -> `TypeError` -> 500. | Punch-in for `EMP9999`. | 404. |
| 14 | `punch_in` | `datetime.fromtimestamp(ms/1000)` is server-local and naive; `ts.date()` is the *server's* date, not IST (R1); no overnight handling; no whole-second truncation. | Server in UTC: a 00:30 IST punch lands on the previous date. | `ms_to_dt` (UTC, whole seconds) + `attendance_date` (IST, overnight rule). |
| 15 | `punch_in` | Check-then-insert is racy and nothing in the database enforces uniqueness. | Two parallel identical punch-ins both return 201. | Unique `(emp_code, date)` index; insert and catch `DuplicateKeyError` -> 409. |
| 16 | `punch_in` response | Returns an `id` (ObjectId string) the contract forbids, and raw datetimes instead of epoch ms. | Compare the response with `AttendanceRecord`. | `record_out()` returns exactly the contract fields. |
| 17 | `list_attendance` | Loads the whole matching set into Python, sorts in Python (by date only, no `emp_code` tie-break), slices; `total = len(docs)`. | ~100k records: seconds and hundreds of MB per request. | `find().sort().skip().limit()` backed by an index; `count_documents` for total. |
| 18 | `list_attendance` | `page=0` gives a negative slice start (wrong rows); no `page_size` cap; `date_from > date_to`, bad dates and unknown `status` not rejected; returns `_id` as `id`; legacy documents without `history`/`half_day` pass through with the fields missing. | `?page=0`, `?date_from=2026-08-01&date_to=2026-07-01`, `?status=XYZ`. | Query constraints and date validation -> 422; `record_out` fills the defaults. |
| 19 | startup | No indexes at all, so every query is a collection scan. | `explain` shows `COLLSCAN`. | `ensure_indexes()` in the lifespan hook, idempotent. |

**Looked at and judged fine**

- `load_dotenv()` does not override variables already in the environment, so injected `MONGO_URI` / `MONGO_DB` win, as required. I kept it, and kept a localhost fallback only for local development.
- The `IST` constant (+05:30) is correct.
- Endpoints are plain `def`, so FastAPI runs them in a thread pool; that is the right match for the synchronous PyMongo client.
- `doc.pop("_id")` after `insert_one`: `_id` is added by the driver, so removing it before returning is correct (the response is now built by `record_out`, which never includes it).
