"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (a local .env is loaded, but real environment variables win)

Layout of this single file
  1. configuration / indexes        5. punch-in, punch-out, regularize
  2. time + rule helpers (R1-R5)    6. attendance list
  3. serialisation helpers          7. analytics (aggregation pipelines)
  4. request models, employees      8. admin explain
"""
import calendar
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError, ServerSelectionTimeoutError

load_dotenv()  # does not override variables that are already set in the environment
log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration and indexes
# --------------------------------------------------------------------------- #
# tz_aware=True: PyMongo hands back aware (UTC) datetimes instead of naive ones.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

MIN_MS, MAX_MS = 100_000_000_000, 4_102_444_800_000
Millis = Annotated[int, Field(strict=True, ge=MIN_MS, le=MAX_MS)]

HHMM = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def ensure_indexes() -> None:
    """Idempotent; runs at startup. Each index is tied to a query in DECISIONS.md."""
    specs = [
        (db.employees, [("emp_code", ASCENDING)], {"unique": True}),
        (db.employees, [("department", ASCENDING), ("emp_code", ASCENDING)], {}),
        (db.employees, [("department", ASCENDING), ("joined_on", ASCENDING)], {}),
        (db.employees, [("joined_on", ASCENDING)], {}),
        # the guarantee behind race-safe punch-in; also serves per-employee scans
        (db.attendance_logs, [("emp_code", ASCENDING), ("date", DESCENDING)], {"unique": True}),
        # GET /attendance sort order, month-wide scans (leaderboard, summaries)
        (db.attendance_logs, [("date", DESCENDING), ("emp_code", ASCENDING)], {}),
        (db.attendance_logs, [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {}),
        # punch-out: "latest record of this employee with punch_in <= t"
        (db.attendance_logs, [("emp_code", ASCENDING), ("punch_in", DESCENDING)], {}),
    ]
    for coll, keys, opts in specs:
        try:
            coll.create_index(keys, **opts)
        except ServerSelectionTimeoutError as exc:  # database down: do not wait 5 s per index
            log.error("MongoDB unreachable, skipping index creation: %s", exc)
            return
        except PyMongoError as exc:  # do not take the whole service down
            log.error("index %s on %s failed: %s", keys, coll.name, exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except Exception as exc:  # pragma: no cover
        log.error("startup index creation failed: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


def unprocessable(msg: str) -> HTTPException:
    return HTTPException(status_code=422, detail=msg)


# --------------------------------------------------------------------------- #
# 2. Time and rule helpers (R1 - R5)
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def now_dt() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    return (aware(dt) - EPOCH) // timedelta(milliseconds=1)


def parse_hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def parse_date_str(s: str) -> date:
    if not isinstance(s, str) or not DATE_RE.fullmatch(s):
        raise ValueError("date must be YYYY-MM-DD")
    return date.fromisoformat(s)  # raises ValueError for 2026-02-30


def date_or_422(s: str, name: str) -> date:
    try:
        return parse_date_str(s)
    except ValueError:
        raise unprocessable(f"{name} must be a valid YYYY-MM-DD date")


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return parse_hhmm(shift_end) <= parse_hhmm(shift_start)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> date:
    """R1: IST calendar date of the punch-in; overnight early-morning punches belong to the previous day."""
    local = aware(punch_in).astimezone(IST)
    d = local.date()
    if is_overnight(shift_start, shift_end) and local.time() < parse_hhmm(shift_end):
        d -= timedelta(days=1)
    return d


def shift_start_at(d: date, shift_start: str) -> datetime:
    return datetime.combine(d, parse_hhmm(shift_start), tzinfo=IST)


def shift_end_at(d: date, shift_start: str, shift_end: str) -> datetime:
    day = d + timedelta(days=1) if is_overnight(shift_start, shift_end) else d
    return datetime.combine(day, parse_hhmm(shift_end), tzinfo=IST)


def compute_late_minutes(punch_in: datetime, d: date, shift_start: str) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then floor(minutes since shift start)."""
    secs = int((aware(punch_in) - shift_start_at(d, shift_start)).total_seconds())
    return secs // 60 if secs > 600 else 0


def compute_overtime(punch_out: datetime, d: date, shift_start: str, shift_end: str) -> int:
    """R3: floor(minutes past shift end), only when at least 30."""
    secs = int((aware(punch_out) - shift_end_at(d, shift_start, shift_end)).total_seconds())
    return secs // 60 if secs >= 1800 else 0


def round_half_up(x: Decimal, places: int) -> Decimal:
    return x.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> Decimal:
    """R4: seconds / 3600, 2 decimals, half-up (exact Decimal arithmetic, no float rounding)."""
    secs = int((aware(punch_out) - aware(punch_in)).total_seconds())
    return round_half_up(Decimal(secs) / Decimal(3600), 2)


def derive_after_punch_out(punch_in: datetime, punch_out: datetime, d: date, shift_start: str, shift_end: str) -> dict:
    wh = compute_work_hours(punch_in, punch_out)
    return {
        "work_hours": float(wh),
        "overtime_minutes": compute_overtime(punch_out, d, shift_start, shift_end),
        "half_day": wh < Decimal("4.50"),  # R5 uses the *rounded* value
    }


# --------------------------------------------------------------------------- #
# 3. Serialisation (BSON -> API)
# --------------------------------------------------------------------------- #
def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": to_ms(doc.get("created_at")),
    }


def _history_out(entry: dict) -> dict:
    changes = {}
    for field, ch in (entry.get("changes") or {}).items():
        if field in ("punch_in", "punch_out"):
            changes[field] = {"from": to_ms(ch.get("from")), "to": to_ms(ch.get("to"))}
        else:
            changes[field] = {"from": ch.get("from"), "to": ch.get("to")}
    return {"at": to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def record_out(doc: dict) -> dict:
    """Contract shape; fills the defaults for legacy documents (no history / half_day ...)."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": to_ms(doc.get("punch_in")),
        "punch_out": to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [_history_out(h) for h in (doc.get("history") or [])],
    }


def get_employee(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 4. Models and employees
# --------------------------------------------------------------------------- #
class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM)
    shift_end: str = Field(default="18:30", pattern=HHMM)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        parse_date_str(v)
        return v

    @model_validator(mode="after")
    def _shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[Millis] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[Millis] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[Millis] = None
    punch_out: Optional[Millis] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="before")
    @classmethod
    def _no_explicit_nulls(cls, data):
        # the contract has no nullable inputs; "omitted" is the only way to say "unchanged"
        if isinstance(data, dict):
            for k in ("status", "punch_in", "punch_out"):
                if k in data and data[k] is None:
                    raise ValueError(f"{k} must not be null")
        return data


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except Exception:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_dt()
    try:
        db.employees.insert_one(doc)  # unique index decides, no check-then-insert
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q: dict = {"department": department} if department else {}
    total = db.employees.count_documents(q)
    cur = db.employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 5. Punch-in, punch-out, regularize
# --------------------------------------------------------------------------- #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_dt()
    d = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": d.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(ts, d, emp["shift_start"]),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date) index: one winner, rest 409
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_dt()
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", DESCENDING)]
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    p_in = aware(rec["punch_in"])
    secs = int((ts - p_in).total_seconds())
    if secs <= 0:
        raise unprocessable("punched_at must be after the punch-in")
    if secs > 24 * 3600:
        raise unprocessable("punch-out is more than 24 hours after the punch-in")
    d = date.fromisoformat(rec["date"])
    fields = {"punch_out": ts, **derive_after_punch_out(p_in, ts, d, emp["shift_start"], emp["shift_end"])}
    # the filter on punch_out: None makes the write itself the arbiter between concurrent requests
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None}, {"$set": fields}, return_document=ReturnDocument.AFTER
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


CHANGE_FIELDS = ["status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day"]


def plan_regularization(rec: dict, emp: dict, body: RegularizeIn) -> dict:
    """Pure function: returns the new field values or raises 422. No database access."""
    status = body.status or rec["status"]
    if status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable("ABSENT/LEAVE records cannot carry punch times")
        p_in = p_out = None
    else:
        p_in = ms_to_dt(body.punch_in) if body.punch_in is not None else rec.get("punch_in")
        p_out = ms_to_dt(body.punch_out) if body.punch_out is not None else rec.get("punch_out")
        if p_in is None:
            raise unprocessable("a presence status requires a punch_in")
        p_in = aware(p_in)
        p_out = aware(p_out) if p_out is not None else None
        d = date.fromisoformat(rec["date"])
        if body.punch_in is not None and attendance_date(p_in, emp["shift_start"], emp["shift_end"]) != d:
            raise unprocessable("punch_in must stay on the record's attendance date")
        if p_out is not None:
            secs = int((p_out - p_in).total_seconds())
            if secs <= 0 or secs > 24 * 3600:
                raise unprocessable("punch_out must be after punch_in and within 24 hours")

    new = {"status": status, "punch_in": p_in, "punch_out": p_out}
    if p_in is not None:
        d = date.fromisoformat(rec["date"])
        new["late_minutes"] = compute_late_minutes(p_in, d, emp["shift_start"])
    else:
        new["late_minutes"] = 0
    if p_in is not None and p_out is not None:
        new.update(derive_after_punch_out(p_in, p_out, d, emp["shift_start"], emp["shift_end"]))
    else:
        new.update({"work_hours": None, "overtime_minutes": 0, "half_day": False})
    return new


def stored_value(rec: dict, field: str):
    v = rec.get(field)
    if field in ("late_minutes", "overtime_minutes"):
        return v or 0
    if field == "half_day":
        return bool(v)
    if field in ("punch_in", "punch_out") and v is not None:
        return aware(v)
    return v


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):  # noqa: A002 - name fixed by the contract
    date_or_422(date, "date")
    emp = get_employee(emp_code)
    for _ in range(5):  # optimistic concurrency: re-read and re-plan if somebody else wrote first
        rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")
        new = plan_regularization(rec, emp, body)
        changes = {
            f: {"from": stored_value(rec, f), "to": new[f]}
            for f in CHANGE_FIELDS
            if stored_value(rec, f) != new[f]
        }
        if not changes:
            raise unprocessable("the request changes nothing")
        entry = {"at": now_dt(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

        history = rec.get("history")
        flt: dict[str, Any] = {"_id": rec["_id"]}
        if isinstance(history, list) and history:
            flt["history"] = {"$size": len(history)}  # nobody appended since we read
            update = {"$set": new, "$push": {"history": entry}}
        else:
            flt["$or"] = [{"history": None}, {"history": {"$size": 0}}]
            update = {"$set": {**new, "history": [entry]}}
        updated = db.attendance_logs.find_one_and_update(flt, update, return_document=ReturnDocument.AFTER)
        if updated is not None:
            return record_out(updated)
    raise HTTPException(409, "concurrent modification, please retry")


# --------------------------------------------------------------------------- #
# 6. Attendance list
# --------------------------------------------------------------------------- #
ATT_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


def attendance_query(emp_code, date_from, date_to, status, page, page_size):
    """Shared by GET /attendance and the explain endpoint so both build the same query."""
    q: dict[str, Any] = {}
    if emp_code:
        q["emp_code"] = emp_code
    lo = date_or_422(date_from, "date_from") if date_from else None
    hi = date_or_422(date_to, "date_to") if date_to else None
    if lo and hi and lo > hi:
        raise unprocessable("date_from must not be after date_to")
    if lo or hi:
        q["date"] = {}
        if lo:
            q["date"]["$gte"] = lo.isoformat()
        if hi:
            q["date"]["$lte"] = hi.isoformat()
    if status:
        q["status"] = status
    return q, (page - 1) * page_size, page_size


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q, skip, limit = attendance_query(emp_code, date_from, date_to, status, page, page_size)
    total = db.attendance_logs.count_documents(q)
    cur = db.attendance_logs.find(q).sort(ATT_SORT).skip(skip).limit(limit)
    return {"items": [record_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 7. Analytics (aggregation pipelines)
# --------------------------------------------------------------------------- #
def month_bounds(month: str) -> tuple[date, date]:
    try:
        first = date(int(month[:4]), int(month[5:7]), 1)
    except ValueError:
        raise unprocessable("month must be a valid YYYY-MM")
    return first, date(first.year, first.month, calendar.monthrange(first.year, first.month)[1])


def count_weekdays(start: date, end: date) -> int:
    n, d = 0, start
    while d <= end:
        n += d.weekday() < 5
        d += timedelta(days=1)
    return n


def q2(x: Decimal) -> float:
    return float(round_half_up(x, 2))


# Reusable aggregation expressions. `p` is the path prefix of the log fields ("" or "logs.").
def x_present(p: str = "") -> dict:
    return {"$in": [f"${p}status", PRESENCE]}


def x_half(p: str = "") -> dict:
    return {"$eq": [{"$ifNull": [f"${p}half_day", False]}, True]}


def x_weekday(p: str = "") -> dict:
    # `date` is a plain YYYY-MM-DD string; read as UTC midnight it gives the right weekday
    dow = {"$isoDayOfWeek": {"$dateFromString": {"dateString": f"${p}date", "format": "%Y-%m-%d"}}}
    return {"$lte": [dow, 5]}


def x_present_half_units(p: str = "") -> dict:
    """Presence on Mon-Fri counted in half-day units: full day = 2, half day = 1 (keeps all sums exact)."""
    return {"$cond": [{"$and": [x_present(p), x_weekday(p)]}, {"$cond": [x_half(p), 1, 2]}, 0]}


def x_late(p: str = "") -> dict:
    return {"$gt": [{"$ifNull": [f"${p}late_minutes", 0]}, 0]}


def monthly_pipeline(emp_code: str, first: date, last: date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present2": {"$sum": x_present_half_units()},
                "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                "late_count": {"$sum": {"$cond": [x_late(), 1, 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=MONTH_PATTERN)):
    emp = get_employee(emp_code)
    first, last = month_bounds(month)
    start = max(first, date.fromisoformat(emp["joined_on"]))
    working = count_weekdays(start, last) if start <= last else 0
    g = next(iter(db.attendance_logs.aggregate(monthly_pipeline(emp_code, first, last))), {})
    p2 = int(g.get("present2", 0))
    pct = None
    if working > 0:
        pct = q2(Decimal(p2) / Decimal(2 * working) * 100)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working,
        "present_days": p2 / 2,
        "leave_days": int(g.get("leave_days", 0)),
        "late_count": int(g.get("late_count", 0)),
        "total_late_minutes": int(g.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(g.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


def summary_pipeline(first: date, last: date, department: Optional[str]) -> list:
    emp_match: dict[str, Any] = {"joined_on": {"$lte": last.isoformat()}}  # R9
    if department:
        emp_match["department"] = department
    L = "logs."
    has_hours = {"$and": [x_present(L), {"$isNumber": "$logs.work_hours"}]}
    return [
        {"$match": emp_match},
        # employees first, so people without any log still produce a row (headcount)
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [{"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}}],
                "as": "logs",
            }
        },
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$department",
                "emps": {"$addToSet": "$emp_code"},
                "present2": {"$sum": x_present_half_units(L)},
                # work hours kept as integer hundredths so the mean can be rounded half-up exactly
                "work_cents": {
                    "$sum": {"$cond": [has_hours, {"$round": [{"$multiply": ["$logs.work_hours", 100]}, 0]}, 0]}
                },
                "work_n": {"$sum": {"$cond": [has_hours, 1, 0]}},
                "late_count": {"$sum": {"$cond": [x_late(L), 1, 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$logs.late_minutes", 0]}},
                "leave_count": {"$sum": {"$cond": [{"$eq": ["$logs.status", "LEAVE"]}, 1, 0]}},
                "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$logs.status", "ON_DUTY"]}, 1, 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": {"$size": "$emps"},
                "present2": 1, "work_cents": 1, "work_n": 1, "late_count": 1,
                "total_late_minutes": 1, "leave_count": 1, "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=MONTH_PATTERN), department: Optional[str] = None):
    first, last = month_bounds(month)
    items = []
    for g in db.employees.aggregate(summary_pipeline(first, last, department)):
        n = int(g["work_n"])
        avg = q2(Decimal(int(g["work_cents"])) / Decimal(n) / 100) if n else None
        items.append(
            {
                "department": g["department"],
                "headcount": int(g["headcount"]),
                "present_days": int(g["present2"]) / 2,
                "avg_work_hours": avg,
                "late_count": int(g["late_count"]),
                "total_late_minutes": int(g["total_late_minutes"]),
                "leave_count": int(g["leave_count"]),
                "on_duty_count": int(g["on_duty_count"]),
            }
        )
    return {"month": month, "items": items}


def leaderboard_pipeline(first: date, last: date, limit: int, department: Optional[str]) -> list:
    pipe: list[dict] = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees disappear here
    ]
    if department:
        pipe.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipe += [
        # $rank is standard competition ranking: 1, 2, 2, 4
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name",
                "department": "$emp.department", "total_late_minutes": 1, "late_count": 1,
            }
        },
    ]
    return pipe


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=MONTH_PATTERN),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    first, last = month_bounds(month)
    rows = db.attendance_logs.aggregate(leaderboard_pipeline(first, last, limit, department))
    return {
        "month": month,
        "items": [
            {
                "rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"],
                "department": r["department"], "total_late_minutes": int(r["total_late_minutes"]),
                "late_count": int(r["late_count"]),
            }
            for r in rows
        ],
    }


def trend_range(from_s: str, to_s: str) -> tuple[date, date]:
    f, t = date_or_422(from_s, "from"), date_or_422(to_s, "to")
    if t < f:
        raise unprocessable("to must not be before from")
    if (t - f).days + 1 > 92:
        raise unprocessable("range must not exceed 92 days")
    return f, t


def trend_pipeline(codes: list, joined: list, f: date, t: date) -> list:
    n = (t - f).days + 1
    f_dt = datetime(f.year, f.month, f.day, tzinfo=UTC)
    half_units = {"$cond": [{"$in": ["$status", PRESENCE]}, {"$cond": [x_half(), 1, 2]}, 0]}
    row_lookup = {"$arrayElemAt": [{"$filter": {"input": "$rows", "as": "r", "cond": {"$eq": ["$$r._id", "$$ds"]}}}, 0]}
    one_day = {
        "$let": {
            "vars": {"dt": {"$add": [f_dt, {"$multiply": ["$$i", 86_400_000]}]}},
            "in": {
                "$let": {
                    "vars": {
                        "ds": {"$dateToString": {"date": "$$dt", "format": "%Y-%m-%d"}},
                        "dow": {"$isoDayOfWeek": "$$dt"},
                    },
                    "in": {
                        "$let": {
                            "vars": {"row": row_lookup},
                            "in": {
                                "date": "$$ds",
                                "wd": {"$lte": ["$$dow", 5]},
                                "headcount": {
                                    "$size": {
                                        "$filter": {"input": {"$literal": joined}, "as": "j", "cond": {"$lte": ["$$j", "$$ds"]}}
                                    }
                                },
                                "p2": {"$ifNull": ["$$row.p2", 0]},
                                "late": {"$ifNull": ["$$row.late", 0]},
                            },
                        }
                    },
                }
            },
        }
    }
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": f.isoformat(), "$lte": t.isoformat()}}},
        # $facet always emits exactly one document, even when no record matched: that is the gap filler's seed
        {"$facet": {"rows": [{"$group": {"_id": "$date", "p2": {"$sum": half_units}, "late": {"$sum": {"$cond": [x_late(), 1, 0]}}}}]}},
        {"$project": {"_id": 0, "days": {"$map": {"input": {"$range": [0, n]}, "as": "i", "in": one_day}}}},
        {"$unwind": "$days"},
        {"$replaceRoot": {"newRoot": "$days"}},
        # attendance_rate in ten-thousandths, rounded half-up with integer-valued arithmetic: floor((p2*10000 + h) / (2h))
        {
            "$addFields": {
                "rate_k": {
                    "$cond": [
                        {"$and": ["$wd", {"$gt": ["$headcount", 0]}]},
                        {"$floor": {"$divide": [{"$add": [{"$multiply": ["$p2", 10000]}, "$headcount"]}, {"$multiply": [2, "$headcount"]}]}},
                        None,
                    ]
                }
            }
        },
        {"$setWindowFields": {"sortBy": {"date": 1}, "output": {"ma_k": {"$avg": "$rate_k", "window": {"documents": [-6, 0]}}}}},
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": "$wd",
                "headcount": 1,
                "present_count": {"$divide": ["$p2", 2]},
                "late_count": "$late",
                "attendance_rate": {"$cond": [{"$eq": ["$rate_k", None]}, None, {"$divide": ["$rate_k", 10000]}]},
                "moving_avg_7d": {
                    "$cond": [
                        {"$eq": [{"$ifNull": ["$ma_k", None]}, None]},
                        None,
                        {"$divide": [{"$floor": {"$add": ["$ma_k", 0.5]}}, 10000]},
                    ]
                },
            }
        },
        {"$sort": {"date": 1}},
    ]


def trend_inputs(department: str, f: date, t: date) -> list:
    emps = list(db.employees.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}))
    return emps


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(..., alias="from"), to: str = Query(...)):
    f, t = trend_range(from_, to)
    emps = trend_inputs(department, f, t)
    if not emps:
        raise HTTPException(404, "unknown department")
    pipe = trend_pipeline([e["emp_code"] for e in emps], [e["joined_on"] for e in emps], f, t)
    items = list(db.attendance_logs.aggregate(pipe))
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 8. Admin: explain
# --------------------------------------------------------------------------- #
def _need(**params):
    missing = [k for k, v in params.items() if v in (None, "")]
    if missing:
        raise unprocessable(f"missing required query parameter(s): {', '.join(missing)}")


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_PATTERN),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Builds the query with the same helpers the real endpoint uses, then asks MongoDB to explain it."""
    if endpoint == "attendance_list":
        q, skip, lim = attendance_query(emp_code, date_from, date_to, status, page, page_size)
        coll = "attendance_logs"
        cmd = {"find": coll, "filter": q, "sort": dict(ATT_SORT), "skip": skip, "limit": lim}
    else:
        if endpoint == "employee_monthly":
            _need(emp_code=emp_code, month=month)
            first, last = month_bounds(month)
            coll, pipe = "attendance_logs", monthly_pipeline(emp_code, first, last)
        elif endpoint == "department_summary":
            _need(month=month)
            first, last = month_bounds(month)
            coll, pipe = "employees", summary_pipeline(first, last, department)
        elif endpoint == "late_leaderboard":
            _need(month=month)
            first, last = month_bounds(month)
            coll, pipe = "attendance_logs", leaderboard_pipeline(first, last, limit, department)
        else:  # department_trend
            _need(department=department, **{"from": from_, "to": to})
            f, t = trend_range(from_, to)
            emps = trend_inputs(department, f, t)
            coll, pipe = "attendance_logs", trend_pipeline([e["emp_code"] for e in emps], [e["joined_on"] for e in emps], f, t)
        cmd = {"aggregate": coll, "pipeline": pipe, "cursor": {}}
    raw = db.command({"explain": cmd, "verbosity": "executionStats"})
    return {"endpoint": endpoint, "collection": coll, "explain": json.loads(json_util.dumps(raw))}
