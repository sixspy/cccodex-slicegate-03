import hashlib, json, secrets, sqlite3, time
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

# ---------------------------------------------------------------------------
# 重切裁决簿（互斥命运，按正确性钉死一边，不得混杂）
#
# 采用方案甲：重切任一步失败 -> 本趟整笔回滚，旧放行态与消费水位保持重切前快照。
# 方案乙（保留已完整且未放行分区行、未齐/冲突行撤回并累计重切跳过次数）不采用。
# 口径一旦写入 recut_ledger，同一业务日当天不得更换；请求偏离裁决簿
# （例如要求乙案或任意混用）一律拒绝并记「重切口径偏了」，不执行任何切片/水位写入。
# 禁止新旧两套正式放行并存；禁止重切中途把水位推到未完整切片。
# ---------------------------------------------------------------------------
PINNED_POLICY = "整笔回滚"
POLICY_LABEL = "方案甲 · 整笔回滚"
POLICY_NOTE = (
    "重切裁决簿口径甲（整笔回滚）：重切失败则整笔回滚本趟，旧放行态与消费水位保持重切前快照；"
    "口径写入后当日内不得更换，偏离即拒绝并留痕；禁止新旧两套正式放行并存；"
    "禁止重切中途把水位推到未完整切片。方案乙（保留完整行、累计重切跳过）不采用。"
)
POLICY_ALIASES = {
    "整笔回滚": "整笔回滚", "回滚": "整笔回滚", "方案甲": "整笔回滚", "整笔回滚本趟": "整笔回滚",
    "a": "整笔回滚", "A": "整笔回滚", "甲": "整笔回滚", "full_rollback": "整笔回滚",
    "保留完整": "保留完整", "保留": "保留完整", "保留已完整": "保留完整", "方案乙": "保留完整",
    "b": "保留完整", "B": "保留完整", "乙": "保留完整", "keep_complete": "保留完整",
}

RESULT_RUNNING = "进行中"
RESULT_SUCCESS = "成功"
RESULT_ROLLBACK = "失败回滚"
RESULT_CONFLICT = "切片号冲突"
RESULT_CONTENTION = "重切争用"
RESULT_DEVIATION = "口径偏离"

LOCK_STALE_MINUTES = 10
LOCK_WAIT_SECONDS = 20
LOCK_POLL_SECONDS = 0.1
# 同一分区在极短窗口内被不同值班接连重切，视为同时点按：后到者必须记争用
CONTENTION_WINDOW_SECONDS = 5


def connect() -> sqlite3.Connection:
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def new_run_no() -> str:
    return "RC-" + datetime.now().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(3)


def completeness_of(row_count: int, expected_count: int) -> str:
    if expected_count > 0 and row_count >= expected_count:
        return "完整"
    return "部分完整" if row_count > 0 else "待检"


def normalize_policy(raw: str | None) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return POLICY_ALIASES.get(text, text)


def ensure_users(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


SCHEMA = """
CREATE TABLE IF NOT EXISTS slices(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slice_no TEXT UNIQUE NOT NULL,
  biz_date TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  row_count INTEGER NOT NULL,
  expected_count INTEGER NOT NULL,
  completeness TEXT NOT NULL,
  release_status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS releases(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slice_id INTEGER NOT NULL,
  consumer TEXT NOT NULL,
  status TEXT NOT NULL,
  note TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS watermarks(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  consumer TEXT NOT NULL,
  biz_date TEXT NOT NULL,
  watermark TEXT NOT NULL,
  source_slice TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recut_ledger(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  biz_date TEXT UNIQUE NOT NULL,
  policy TEXT NOT NULL,
  note TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recut_runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_no TEXT UNIQUE NOT NULL,
  biz_date TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  policy TEXT NOT NULL,
  status TEXT NOT NULL,
  requested_by TEXT NOT NULL,
  consumer TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  finished_at TEXT,
  note TEXT NOT NULL DEFAULT '',
  snapshot_json TEXT NOT NULL DEFAULT '{}',
  result_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS recut_run_items(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  slice_no TEXT NOT NULL,
  slice_id INTEGER,
  row_count INTEGER,
  expected_count INTEGER,
  status TEXT NOT NULL,
  reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS partition_locks(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  biz_date TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  owner TEXT NOT NULL,
  token TEXT NOT NULL,
  run_no TEXT NOT NULL,
  acquired_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  UNIQUE(biz_date, partition_key)
);
CREATE TABLE IF NOT EXISTS recut_inflight(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  biz_date TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  owner TEXT NOT NULL,
  token TEXT NOT NULL,
  arrived_at TEXT NOT NULL,
  UNIQUE(biz_date, partition_key, token)
);
CREATE INDEX IF NOT EXISTS idx_recut_runs_part ON recut_runs(biz_date, partition_key, id);
CREATE INDEX IF NOT EXISTS idx_recut_items_run ON recut_run_items(run_id);
CREATE INDEX IF NOT EXISTS idx_slices_part ON slices(biz_date, partition_key, id);
"""


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(r["name"] == column for r in conn.execute(f"PRAGMA table_info({table})"))


def _recover_orphans(conn: sqlite3.Connection) -> None:
    """进程中断：过期分区锁与在途登记清理；未收口的重切按口径甲关闭为失败回滚。"""
    conn.execute("DELETE FROM partition_locks WHERE expires_at < ?", (now_iso(),))
    conn.execute("DELETE FROM recut_inflight WHERE arrived_at < ?",
                 ((datetime.now() - timedelta(minutes=LOCK_STALE_MINUTES)).isoformat(timespec="milliseconds"),))
    cutoff = (datetime.now() - timedelta(minutes=LOCK_STALE_MINUTES)).isoformat(timespec="milliseconds")
    conn.execute(
        "UPDATE recut_runs SET status=?, finished_at=?, note=note || '；进程中断，按口径甲整笔回滚' "
        "WHERE status=? AND created_at < ?",
        (RESULT_ROLLBACK, now_iso(), RESULT_RUNNING, cutoff),
    )


def init_db() -> None:
    with closing(connect()) as conn:
        ensure_users(conn)
        conn.executescript(SCHEMA)
        for col, decl in (("superseded_by", "TEXT"), ("recut_run_id", "INTEGER"), ("retired_at", "TEXT")):
            if not _has_column(conn, "slices", col):
                conn.execute(f"ALTER TABLE slices ADD COLUMN {col} {decl}")
        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for r in rows:
                conn.execute(
                    "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status)"
                    " VALUES (?,?,?,?,?,?,?)", r)
            conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at)"
                         " VALUES (2,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')")
            conn.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice)"
                         " VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-west')")
        _recover_orphans(conn)
        conn.commit()


@app.on_event("startup")
def _start() -> None:
    init_db()


# ---------------------------------------------------------------------------
# 重切裁决簿 / 分区锁 基础操作
# ---------------------------------------------------------------------------
def _insert_run(conn: sqlite3.Connection, biz_date: str, partition_key: str, policy: str, status: str,
                user: str, consumer: str, note: str, snapshot: dict | None = None,
                result: dict | None = None, finished: bool = True) -> sqlite3.Row:
    run_no = new_run_no()
    ts = now_iso()
    conn.execute(
        "INSERT INTO recut_runs(run_no,biz_date,partition_key,policy,status,requested_by,consumer,"
        "created_at,finished_at,note,snapshot_json,result_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (run_no, biz_date, partition_key, policy, status, user, consumer, ts,
         ts if finished else None, note, json.dumps(snapshot or {}, ensure_ascii=False),
         json.dumps(result or {}, ensure_ascii=False)),
    )
    return conn.execute("SELECT * FROM recut_runs WHERE run_no=?", (run_no,)).fetchone()


def _insert_item(conn: sqlite3.Connection, run_id: int, slice_no: str, slice_id: int | None,
                 row_count: int | None, expected_count: int | None, status: str, reason: str) -> None:
    conn.execute(
        "INSERT INTO recut_run_items(run_id,slice_no,slice_id,row_count,expected_count,status,reason)"
        " VALUES (?,?,?,?,?,?,?)",
        (run_id, slice_no, slice_id, row_count, expected_count, status, reason),
    )


def _run_payload(conn: sqlite3.Connection, run: sqlite3.Row | dict) -> dict:
    data = dict(run)
    data["items"] = [dict(r) for r in conn.execute(
        "SELECT * FROM recut_run_items WHERE run_id=? ORDER BY id", (data["id"],))]
    for key in ("snapshot", "result"):
        try:
            data[key] = json.loads(data.get(f"{key}_json") or "{}")
        except ValueError:
            data[key] = {}
    return data


def _read_ledger(conn: sqlite3.Connection, biz_date: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM recut_ledger WHERE biz_date=?", (biz_date,)).fetchone()


def _pin_ledger(biz_date: str, user: str) -> sqlite3.Row:
    with closing(connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = _read_ledger(conn, biz_date)
            if row is None:
                conn.execute(
                    "INSERT INTO recut_ledger(biz_date,policy,note,created_by,created_at) VALUES (?,?,?,?,?)",
                    (biz_date, PINNED_POLICY, POLICY_NOTE, user, now_iso()))
                row = _read_ledger(conn, biz_date)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            row = _read_ledger(conn, biz_date)
        return row


def _record_run_only(biz_date: str, partition_key: str, policy: str, status: str, user: str,
                     consumer: str, note: str, items: list[dict] | None = None,
                     snapshot: dict | None = None, result: dict | None = None) -> dict:
    with closing(connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            run = _insert_run(conn, biz_date, partition_key, policy, status, user, consumer, note,
                              snapshot=snapshot, result=result)
            for it in items or []:
                _insert_item(conn, run["id"], it["slice_no"], it.get("slice_id"), it.get("row_count"),
                             it.get("expected_count"), it["status"], it["reason"])
            conn.commit()
            return _run_payload(conn, run)
        except Exception:
            conn.rollback()
            raise


def _refresh_run(run_id: int, status: str, note: str, items: list[dict],
                 result: dict | None = None) -> dict:
    """收口一次重切。明细写入是尽力而为；运行状态与结果必须落盘，不能卡在「进行中」。"""
    with closing(connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for it in items:
                try:
                    _insert_item(conn, run_id, it["slice_no"], it.get("slice_id"), it.get("row_count"),
                                 it.get("expected_count"), it["status"], it["reason"])
                except Exception:
                    continue
            if result is None:
                conn.execute("UPDATE recut_runs SET status=?, finished_at=?, note=? WHERE id=?",
                             (status, now_iso(), note, run_id))
            else:
                conn.execute("UPDATE recut_runs SET status=?, finished_at=?, note=?, result_json=? WHERE id=?",
                             (status, now_iso(), note, json.dumps(result, ensure_ascii=False), run_id))
            conn.commit()
            run = conn.execute("SELECT * FROM recut_runs WHERE id=?", (run_id,)).fetchone()
            return _run_payload(conn, run)
        except Exception:
            conn.rollback()
            raise


def _running_run(conn: sqlite3.Connection, biz_date: str, partition_key: str) -> sqlite3.Row | None:
    """同分区是否仍有进行中的重切（持锁执行中才算重叠）。"""
    return conn.execute(
        "SELECT * FROM recut_runs WHERE biz_date=? AND partition_key=? AND status=? ORDER BY id DESC LIMIT 1",
        (biz_date, partition_key, RESULT_RUNNING)).fetchone()


def _peer_inflight(conn: sqlite3.Connection, biz_date: str, partition_key: str,
                   token: str, my_id: int) -> sqlite3.Row | None:
    """比我先登记、仍在途的同分区重切：说明我才是后到者。"""
    return conn.execute(
        "SELECT * FROM recut_inflight WHERE biz_date=? AND partition_key=? AND token<>? AND id<?"
        " ORDER BY id LIMIT 1", (biz_date, partition_key, token, my_id)).fetchone()


def _register_inflight(conn: sqlite3.Connection, biz_date: str, partition_key: str,
                       owner: str, token: str) -> int:
    cur = conn.execute(
        "INSERT INTO recut_inflight(biz_date,partition_key,owner,token,arrived_at) VALUES (?,?,?,?,?)",
        (biz_date, partition_key, owner, token, now_iso()))
    return cur.lastrowid


def _recent_peer_run(conn: sqlite3.Connection, biz_date: str, partition_key: str,
                     owner: str) -> sqlite3.Row | None:
    """另一名值班在争用窗口内刚点过同分区重切：同时点按的兜底判定（绝不静默通过）。"""
    cutoff = (datetime.now() - timedelta(seconds=CONTENTION_WINDOW_SECONDS)).isoformat(timespec="milliseconds")
    return conn.execute(
        "SELECT * FROM recut_runs WHERE biz_date=? AND partition_key=? AND requested_by<>?"
        " AND status=? AND created_at>=? ORDER BY id DESC LIMIT 1",
        (biz_date, partition_key, owner, RESULT_SUCCESS, cutoff)).fetchone()


def _clear_inflight(conn: sqlite3.Connection, biz_date: str, partition_key: str, token: str) -> None:
    conn.execute("DELETE FROM recut_inflight WHERE biz_date=? AND partition_key=? AND token=?",
                 (biz_date, partition_key, token))


def _acquire_lease(biz_date: str, partition_key: str, owner: str, policy: str = PINNED_POLICY) -> dict:
    """同业务日同分区重切互斥：先到者持租约；后到者先记「重切争用」再排队，绝不静默通过。"""
    token = secrets.token_hex(8)
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    contended: dict | None = None
    contended_run: dict | None = None
    with closing(connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            _clear_inflight(conn, biz_date, partition_key, token)
            my_id = _register_inflight(conn, biz_date, partition_key, owner, token)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    try:
        while True:
            with closing(connect()) as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    held = conn.execute(
                        "SELECT * FROM partition_locks WHERE biz_date=? AND partition_key=?",
                        (biz_date, partition_key)).fetchone()
                    fresh = bool(held and held["expires_at"] >= now_iso())
                    other = None if fresh else _running_run(conn, biz_date, partition_key)
                    peer = _peer_inflight(conn, biz_date, partition_key, token, my_id)
                    recent = None if (fresh or other is not None or peer is not None) else \
                        _recent_peer_run(conn, biz_date, partition_key, owner)
                    if fresh or other is not None or peer is not None or recent is not None:
                        if contended is None:
                            if held:
                                contended = {
                                    "owner": held["owner"], "run_no": held["run_no"],
                                    "acquired_at": held["acquired_at"], "expires_at": held["expires_at"],
                                }
                                detail = f"{held['owner']}（{held['run_no']}）持锁"
                            elif other is not None:
                                contended = {"owner": other["requested_by"], "run_no": other["run_no"],
                                             "acquired_at": other["created_at"],
                                             "finished_at": other["finished_at"],
                                             "expires_at": None}
                                detail = f"{other['requested_by']}（{other['run_no']}）正执行"
                            elif peer is not None:
                                contended = {"owner": peer["owner"], "run_no": "",
                                             "acquired_at": peer["arrived_at"],
                                             "finished_at": None, "expires_at": None}
                                detail = f"{peer['owner']}（同时在途）"
                            else:
                                contended = {"owner": recent["requested_by"], "run_no": recent["run_no"],
                                             "acquired_at": recent["created_at"],
                                             "finished_at": recent["finished_at"],
                                             "expires_at": None}
                                detail = f"{recent['requested_by']}（{recent['run_no']}）刚完成"
                            note = (f"{owner} 与 {detail} 同业务日同分区同时点重切，"
                                    "后到者加锁排队并记重切争用，未静默通过")
                            run = _insert_run(conn, biz_date, partition_key, policy,
                                              RESULT_CONTENTION, owner, "", note)
                            conn.commit()
                            contended_run = _run_payload(conn, run)
                            continue
                        conn.commit()
                    else:
                        if held:
                            conn.execute("DELETE FROM partition_locks WHERE id=?", (held["id"],))
                        ts = now_iso()
                        conn.execute(
                            "INSERT INTO partition_locks(biz_date,partition_key,owner,token,run_no,"
                            "acquired_at,expires_at) VALUES (?,?,?,?,?,?,?)",
                            (biz_date, partition_key, owner, token, "", ts,
                             (datetime.now() + timedelta(minutes=LOCK_STALE_MINUTES)).isoformat(timespec="milliseconds")))
                        _clear_inflight(conn, biz_date, partition_key, token)
                        conn.commit()
                        return {"token": token, "contended": contended,
                                "contended_run": contended_run}
                except Exception:
                    conn.rollback()
                    raise
            if time.monotonic() >= deadline:
                with closing(connect()) as conn:
                    _clear_inflight(conn, biz_date, partition_key, token)
                    conn.commit()
                return {"token": None, "contended": contended,
                        "contended_run": contended_run, "timed_out": True}
            time.sleep(LOCK_POLL_SECONDS)
    except Exception:
        with closing(connect()) as conn:
            _clear_inflight(conn, biz_date, partition_key, token)
            conn.commit()
        raise


def _bind_lease_run(biz_date: str, partition_key: str, token: str, run_no: str) -> None:
    with closing(connect()) as conn:
        conn.execute("UPDATE partition_locks SET run_no=? WHERE biz_date=? AND partition_key=? AND token=?",
                     (run_no, biz_date, partition_key, token))
        conn.commit()


def _release_lease(biz_date: str, partition_key: str, token: str | None) -> None:
    if not token:
        return
    with closing(connect()) as conn:
        conn.execute("DELETE FROM partition_locks WHERE biz_date=? AND partition_key=? AND token=?",
                     (biz_date, partition_key, token))
        conn.commit()


# ---------------------------------------------------------------------------
# 快照（甲案的唯一恢复基准）
# ---------------------------------------------------------------------------
def _partition_snapshot(conn: sqlite3.Connection, biz_date: str, partition_key: str) -> dict:
    slices = [dict(r) for r in conn.execute(
        "SELECT * FROM slices WHERE biz_date=? AND partition_key=? ORDER BY id", (biz_date, partition_key))]
    slice_ids = [s["id"] for s in slices]
    releases: list[dict] = []
    if slice_ids:
        qs = ",".join("?" * len(slice_ids))
        releases = [dict(r) for r in conn.execute(
            f"SELECT * FROM releases WHERE slice_id IN ({qs}) ORDER BY id", slice_ids)]
    watermarks = []
    if slices:
        nos = [s["slice_no"] for s in slices]
        qs = ",".join("?" * len(nos))
        watermarks = [dict(r) for r in conn.execute(
            f"SELECT * FROM watermarks WHERE source_slice IN ({qs}) ORDER BY id", nos)]
    return {
        "biz_date": biz_date, "partition_key": partition_key,
        "slices": slices, "releases": releases, "watermarks": watermarks,
        "released_slices": sorted(s["slice_no"] for s in slices if s["release_status"] == "已放行"),
        "watermark_head": ({w["consumer"]: w["source_slice"] for w in watermarks} or {}),
        "taken_at": now_iso(),
    }


def _snapshot_digest(snapshot: dict) -> dict:
    return {
        "slices": sorted((s["id"], s["slice_no"], s["row_count"], s["expected_count"],
                          s["completeness"], s["release_status"], s.get("retired_at") or "")
                         for s in snapshot.get("slices", [])),
        "releases": sorted((r["id"], r["slice_id"], r["consumer"], r["status"])
                           for r in snapshot.get("releases", [])),
        "watermarks": sorted((w["id"], w["consumer"], w["watermark"], w["source_slice"])
                             for w in snapshot.get("watermarks", [])),
    }


def _snapshot_intact(conn: sqlite3.Connection, snapshot: dict) -> bool:
    return _snapshot_digest(_partition_snapshot(conn, snapshot["biz_date"], snapshot["partition_key"])) \
        == _snapshot_digest(snapshot)


def _check_snapshot_intact(snapshot: dict) -> bool:
    with closing(connect()) as conn:
        return _snapshot_intact(conn, snapshot)


class RecutSliceIn(BaseModel):
    slice_no: str
    row_count: int
    expected_count: int


class RecutIn(BaseModel):
    biz_date: str
    partition_key: str
    policy: str | None = None
    consumer: str = "ads_order"
    slices: list[RecutSliceIn]


@app.post("/api/recut")
def recut(body: RecutIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    biz_date, partition_key = body.biz_date.strip(), body.partition_key.strip()
    if not biz_date or not partition_key:
        raise HTTPException(400, "业务日与分区不能为空")
    if not body.slices:
        raise HTTPException(400, "重切清单为空，整次拒绝")

    wanted = normalize_policy(body.policy)

    # 1) 裁决簿：首次重切即钉死口径，同日不得更换；偏离则拒绝并记「重切口径偏了」
    ledger = _pin_ledger(biz_date, user)
    pinned = ledger["policy"]
    if wanted is not None and wanted != pinned:
        run = _record_run_only(
            biz_date, partition_key, pinned, RESULT_DEVIATION, user, body.consumer,
            f"重切口径偏了：请求 {wanted}，重切裁决簿钉死 {pinned}，整次拒绝未执行")
        return JSONResponse(status_code=409, content={
            "ok": False, "error": RESULT_DEVIATION, "policy": pinned,
            "message": "重切口径偏了：与重切裁决簿不一致，已拒绝并留痕，未写入任何切片或水位",
            "pinned_policy": pinned, "wanted_policy": wanted, "run": run})

    # 2) 同业务日同分区互斥：先到者持租约执行，后到者加锁排队且必须留痕「重切争用」
    lease = _acquire_lease(biz_date, partition_key, user, pinned)
    if lease.get("timed_out"):
        run = lease.get("contended_run")
        if not run:
            run = _record_run_only(
                biz_date, partition_key, pinned, RESULT_CONTENTION, user, body.consumer,
                "重切争用：同分区锁等待超时，未获得执行权，已留痕未静默通过")
        return JSONResponse(status_code=409, content={
            "ok": False, "error": RESULT_CONTENTION, "policy": pinned,
            "message": "同分区重切互斥，排队超时，已记重切争用",
            "holder": lease.get("contended"), "run": run})
    token = lease["token"]
    contended_run = lease.get("contended_run")
    snapshot: dict = {}
    run_id: int | None = None
    try:
        # 3) 先取快照并登记「进行中」，队列/详情立刻可见，水位仍停在重切前
        with closing(connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                snapshot = _partition_snapshot(conn, biz_date, partition_key)
                run = _insert_run(conn, biz_date, partition_key, pinned, RESULT_RUNNING, user,
                                  body.consumer, "重切执行中：旧放行态与水位已取快照，尚未写入新切片",
                                  snapshot=snapshot, finished=False)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        run_id = run["id"]
        _bind_lease_run(biz_date, partition_key, token, run["run_no"])

        # 4) 同业务日同分区分片号冲突：整次失败闭，不落任何新切片
        conflicts: list[dict] = []
        wanted_nos = [s.slice_no.strip() for s in body.slices]
        dup = sorted({n for n in wanted_nos if n and wanted_nos.count(n) > 1})
        with closing(connect()) as conn:
            for s in body.slices:
                no = s.slice_no.strip()
                reason, slice_id = None, None
                if not no:
                    reason = "切片号为空，整次失败回滚"
                elif no in dup:
                    reason = "本趟重切清单内切片号重复"
                else:
                    existing = conn.execute("SELECT * FROM slices WHERE slice_no=?", (no,)).fetchone()
                    if existing:
                        slice_id = existing["id"]
                        same_part = (existing["biz_date"] == biz_date
                                     and existing["partition_key"] == partition_key)
                        reason = ("同业务日同分区分片号冲突" if same_part
                                  else "切片号已被其他业务日/分区占用")
                if reason:
                    conflicts.append({"slice_no": no or "(空)", "slice_id": slice_id,
                                      "row_count": s.row_count, "expected_count": s.expected_count,
                                      "status": "冲突", "reason": reason})

        if conflicts:
            intact = _check_snapshot_intact(snapshot)
            run = _refresh_run(
                run_id, RESULT_CONFLICT,
                f"切片号冲突 {len(conflicts)} 项，按口径甲整笔回滚本趟；旧放行态与水位保持重切前快照",
                conflicts, result={"rollback": True, "snapshot_intact": intact})
            _release_lease(biz_date, partition_key, token)
            return JSONResponse(status_code=409, content={
                "ok": False, "error": RESULT_CONFLICT, "policy": pinned,
                "message": "同业务日同分区分片号冲突，整次重切失败回滚，未产生新切片",
                "snapshot": snapshot, "run": run, "contention": contended_run})

        # 5) 任一新切片未完整：禁止半写落库，甲案整笔回滚（已完整行也不保留）
        not_complete = [s for s in body.slices
                        if completeness_of(s.row_count, s.expected_count) != "完整"]
        if not_complete:
            items = []
            for s in body.slices:
                comp = completeness_of(s.row_count, s.expected_count)
                items.append({
                    "slice_no": s.slice_no.strip(), "slice_id": None,
                    "row_count": s.row_count, "expected_count": s.expected_count,
                    "status": "未齐" if comp != "完整" else "未保留",
                    "reason": ("完整度未达标，禁止半写切片落库" if comp != "完整"
                               else "同趟存在未齐切片，按口径甲整笔回滚，已完整行亦不予保留")})
            intact = _check_snapshot_intact(snapshot)
            run = _refresh_run(
                run_id, RESULT_ROLLBACK,
                f"重切失败：{len(not_complete)} 片未齐，整笔回滚本趟；旧放行态与消费水位保持重切前快照",
                items, result={"rollback": True, "snapshot_intact": intact})
            _release_lease(biz_date, partition_key, token)
            return JSONResponse(status_code=409, content={
                "ok": False, "error": RESULT_ROLLBACK, "policy": pinned,
                "message": "重切失败：完整度未达标，整笔回滚；旧放行态与水位保持重切前快照",
                "snapshot": snapshot, "run": run, "contention": contended_run})

        # 6) 成功：同一事务内旧切片整批撤回 + 新切片落库 + 放行接续 + 水位推进
        with closing(connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                old_rows = [dict(r) for r in conn.execute(
                    "SELECT * FROM slices WHERE biz_date=? AND partition_key=? AND retired_at IS NULL"
                    " ORDER BY id", (biz_date, partition_key))]
                released_old = [o for o in old_rows if o["release_status"] == "已放行"]
                carry_consumers = sorted({r["consumer"] for r in conn.execute(
                    "SELECT DISTINCT consumer FROM releases WHERE status='成功' AND slice_id IN "
                    "(SELECT id FROM slices WHERE biz_date=? AND partition_key=? AND release_status='已放行')",
                    (biz_date, partition_key))})
                if released_old and not carry_consumers:
                    carry_consumers = [body.consumer]

                ts = now_iso()
                retired_items: list[dict] = []
                for o in old_rows:
                    state = "已撤回" if o["release_status"] == "已放行" else "已归档"
                    conn.execute("UPDATE slices SET release_status=?, retired_at=?, superseded_by=? WHERE id=?",
                                 (state, ts, run["run_no"], o["id"]))
                    for c in carry_consumers:
                        conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at)"
                                     " VALUES (?,?,?,?,?)",
                                     (o["id"], c, "撤回", "重切成功，旧版本正式放行整片撤回", ts))
                    retired_items.append({"slice_no": o["slice_no"], "slice_id": o["id"],
                                          "row_count": o["row_count"], "expected_count": o["expected_count"],
                                          "status": state, "reason": f"被重切 {run['run_no']} 替换"})

                new_items: list[dict] = []
                new_rows: list[dict] = []
                for s in body.slices:
                    no = s.slice_no.strip()
                    comp = completeness_of(s.row_count, s.expected_count)
                    rel = "已放行" if carry_consumers else "未放行"
                    cur = conn.execute(
                        "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,"
                        "completeness,release_status,recut_run_id) VALUES (?,?,?,?,?,?,?,?)",
                        (no, biz_date, partition_key, s.row_count, s.expected_count, comp, rel, run_id))
                    sid = cur.lastrowid
                    for c in carry_consumers:
                        conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at)"
                                     " VALUES (?,?,?,?,?)",
                                     (sid, c, "成功", f"重切完整，放行接续 {run['run_no']}", ts))
                    new_rows.append({"id": sid, "slice_no": no, "release_status": rel,
                                     "completeness": comp})
                    new_items.append({"slice_no": no, "slice_id": sid, "row_count": s.row_count,
                                      "expected_count": s.expected_count, "status": "成功",
                                      "reason": ("完整切片，旧放行态接续" if carry_consumers
                                                 else "完整切片，待放行")})

                # 水位只在新切片全量完整且已落库后推进；来源必为完整且已放行切片
                watermark_rows: list[dict] = []
                if carry_consumers and new_rows:
                    head = max(new_rows, key=lambda r: r["id"])
                    for c in carry_consumers:
                        cur = conn.execute(
                            "INSERT INTO watermarks(consumer,biz_date,watermark,source_slice)"
                            " VALUES (?,?,?,?)", (c, biz_date, ts, head["slice_no"]))
                        watermark_rows.append({"id": cur.lastrowid, "consumer": c,
                                               "source_slice": head["slice_no"]})

                result = {"policy": pinned, "new_slices": new_rows,
                          "retired": [o["slice_no"] for o in old_rows],
                          "consumers": carry_consumers, "watermarks": watermark_rows,
                          "snapshot_taken_at": snapshot["taken_at"], "rollback": False}
                note = ("重切成功：旧切片整片撤回（不再保留正式放行），新完整切片接续放行；"
                        "新水位仅指向完整且已放行切片")
                for it in retired_items + new_items:
                    _insert_item(conn, run_id, it["slice_no"], it.get("slice_id"), it.get("row_count"),
                                 it.get("expected_count"), it["status"], it["reason"])
                conn.execute("UPDATE recut_runs SET status=?, finished_at=?, note=?, result_json=? WHERE id=?",
                             (RESULT_SUCCESS, ts, note, json.dumps(result, ensure_ascii=False), run_id))
                conn.commit()
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                intact = _check_snapshot_intact(snapshot)
                items = [{"slice_no": s.slice_no.strip() or "(空)", "status": "冲突",
                          "row_count": s.row_count, "expected_count": s.expected_count,
                          "reason": f"落库时切片号冲突（{exc}），整笔回滚"} for s in body.slices]
                run = _refresh_run(
                    run_id, RESULT_CONFLICT,
                    "切片号冲突，按口径甲整笔回滚本趟；旧放行态与水位保持重切前快照",
                    items, result={"rollback": True, "snapshot_intact": intact})
                _release_lease(biz_date, partition_key, token)
                return JSONResponse(status_code=409, content={
                    "ok": False, "error": RESULT_CONFLICT, "policy": pinned,
                    "message": "切片号冲突，整次重切失败回滚", "snapshot": snapshot,
                    "run": run, "contention": contended_run})
            except Exception:
                conn.rollback()
                raise
    except HTTPException:
        _release_lease(biz_date, partition_key, token)
        raise
    except Exception as exc:
        _release_lease(biz_date, partition_key, token)
        intact = _check_snapshot_intact(snapshot) if snapshot else True
        items = [{"slice_no": s.slice_no.strip() or "(空)", "status": "失败",
                  "row_count": s.row_count, "expected_count": s.expected_count,
                  "reason": "重切异常，整笔回滚"} for s in body.slices]
        run = None
        if run_id is not None:
            try:
                run = _refresh_run(run_id, RESULT_ROLLBACK,
                                   f"重切异常，按口径甲整笔回滚：{exc}", items,
                                   result={"rollback": True, "snapshot_intact": intact})
            except Exception:
                run = None
        return JSONResponse(status_code=500, content={
            "ok": False, "error": RESULT_ROLLBACK, "policy": pinned,
            "message": f"重切异常，已整笔回滚，旧放行态与水位保持重切前快照：{exc}",
            "snapshot_intact": intact, "run": run, "contention": contended_run})

    _release_lease(biz_date, partition_key, token)
    with closing(connect()) as conn:
        final = conn.execute("SELECT * FROM recut_runs WHERE id=?", (run_id,)).fetchone()
        payload = _run_payload(conn, final)
    return {"ok": True, "error": None, "policy": pinned, "run": payload,
            "result": payload.get("result", {}), "contention": contended_run}


# ---------------------------------------------------------------------------
# 查询接口
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health():
    with closing(connect()) as c:
        slices = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
        running = c.execute("SELECT COUNT(*) c FROM recut_runs WHERE status=?", (RESULT_RUNNING,)).fetchone()["c"]
        locks = c.execute("SELECT COUNT(*) c FROM partition_locks").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": slices,
            "recut_running": running, "partition_locks": locks,
            "recut_policy": PINNED_POLICY, "db": str(DB_PATH)}


@app.post("/api/login")
def login(body: LoginIn):
    with closing(connect()) as c:
        row = c.execute("SELECT username, role FROM users WHERE username=? AND password_sha=?",
                        (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM slices ORDER BY id")]
        locks = {(r["biz_date"], r["partition_key"]): dict(r)
                 for r in c.execute("SELECT * FROM partition_locks")}
    for d in rows:
        lock = locks.get((d["biz_date"], d["partition_key"]))
        locked = bool(lock and lock["expires_at"] >= now_iso())
        d["recut_locked"] = locked
        d["lock_owner"] = lock["owner"] if locked else None
        d["active"] = not bool(d["retired_at"])
    return {"items": rows}


class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int


@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = completeness_of(body.row_count, body.expected_count)
    with closing(connect()) as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,"
                "completeness,release_status) VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count,
                 body.expected_count, comp, "未放行"),
            )
            c.commit()
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")


@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        if row["retired_at"]:
            raise HTTPException(409, "切片已被重切撤回，不可重裁")
        comp = completeness_of(row["row_count"], row["expected_count"])
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
        c.commit()
    return {"id": sid, "completeness": comp}


class ReleaseIn(BaseModel):
    consumer: str


def _partition_busy(c: sqlite3.Connection, biz_date: str, partition_key: str) -> bool:
    """该分区是否有重切持锁或在途：正式放行不得与重切交叉，避免新旧两套并存。"""
    lock = c.execute("SELECT * FROM partition_locks WHERE biz_date=? AND partition_key=?",
                     (biz_date, partition_key)).fetchone()
    if lock and lock["expires_at"] >= now_iso():
        return True
    running = _running_run(c, biz_date, partition_key)
    return running is not None


@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    ts = now_iso()
    with closing(connect()) as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
            if not row:
                c.rollback()
                raise HTTPException(404, "切片不存在")
            if row["retired_at"]:
                c.rollback()
                raise HTTPException(409, "切片已被重切撤回，禁止放行")
            if _partition_busy(c, row["biz_date"], row["partition_key"]):
                c.rollback()
                raise HTTPException(409, "该分区重切进行中，禁止同时放行，避免新旧两套正式放行并存")
            if row["completeness"] != "完整":
                c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                          (sid, body.consumer, "拒绝", "完整度未达标", ts))
                c.commit()
                raise HTTPException(409, "未完整禁止放行")
            if row["release_status"] == "已放行":
                c.commit()
                return {"ok": True, "already": True, "message": "切片已放行，未重复推水位"}
            c.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (sid,))
            c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                      (sid, body.consumer, "成功", "完整切片放行", ts))
            c.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES (?,?,?,?)",
                      (body.consumer, row["biz_date"], ts, row["slice_no"]))
            c.commit()
        except HTTPException:
            raise
        except Exception:
            c.rollback()
            raise
    return {"ok": True}


@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        rows = c.execute("SELECT r.*, s.slice_no, s.retired_at FROM releases r"
                         " JOIN slices s ON s.id=r.slice_id ORDER BY r.id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


def _watermark_view(conn: sqlite3.Connection) -> list[dict]:
    rows = [dict(r) for r in conn.execute(
        "SELECT w.*, s.id AS source_slice_id, s.release_status AS source_release_status,"
        " s.completeness AS source_completeness, s.retired_at AS source_retired_at,"
        " s.superseded_by AS source_superseded_by, s.partition_key AS source_partition_key"
        " FROM watermarks w LEFT JOIN slices s ON s.slice_no=w.source_slice"
        " ORDER BY w.id DESC")]
    # 每个消费方+业务日的当前水位 head
    head_ids = {}
    for r in rows:
        head_ids.setdefault((r["consumer"], r["biz_date"]), r["id"])
    for d in rows:
        if d["source_release_status"] is None:
            d["align"] = "来源缺失"
        elif d["source_release_status"] == "已放行":
            d["align"] = "一致"
        elif d["source_release_status"] in ("已撤回", "已归档"):
            d["align"] = "来源已撤回"
        else:
            d["align"] = "来源未放行"
        d["is_head"] = head_ids.get((d["consumer"], d["biz_date"])) == d["id"]
        if d["source_release_status"] == "已放行":
            d["source_state"] = "正式放行"
        elif d["source_release_status"] in ("已撤回", "已归档"):
            d["source_state"] = "重切撤回"
        else:
            d["source_state"] = "未放行"
    return rows


@app.get("/api/watermarks")
def list_wm(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        rows = _watermark_view(c)
    return {"policy": PINNED_POLICY, "note": POLICY_NOTE, "items": rows}


@app.get("/api/recut/policies")
def recut_policies(authorization: str | None = Header(default=None)):
    require_user(authorization)
    return {"pinned": PINNED_POLICY, "policy": PINNED_POLICY, "label": POLICY_LABEL,
            "note": POLICY_NOTE, "not_adopted": "保留完整", "aliases": POLICY_ALIASES,
            "skip_count_note": "口径甲不回填重切跳过次数；重切跳过计数属于未采用的方案乙"}


@app.get("/api/recut/ledger")
def recut_ledger(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM recut_ledger ORDER BY biz_date")]
        locks = [dict(r) for r in c.execute("SELECT * FROM partition_locks ORDER BY id")]
    return {"pinned": PINNED_POLICY, "label": POLICY_LABEL, "note": POLICY_NOTE,
            "items": rows, "locks": locks}


def _recut_runs(conn: sqlite3.Connection, biz_date: str | None, partition_key: str | None,
                limit: int) -> list[dict]:
    sql = "SELECT * FROM recut_runs WHERE 1=1"
    args: list[Any] = []
    if biz_date:
        sql += " AND biz_date=?"; args.append(biz_date)
    if partition_key:
        sql += " AND partition_key=?"; args.append(partition_key)
    sql += " ORDER BY id DESC LIMIT ?"; args.append(max(1, min(limit, 500)))
    return [_run_payload(conn, r) for r in conn.execute(sql, args)]


@app.get("/api/recut/runs")
def recut_runs(biz_date: str | None = None, partition_key: str | None = None,
               limit: int = 100, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        runs = _recut_runs(c, biz_date, partition_key, limit)
        contention = {(r["biz_date"], r["partition_key"]): r["c"] for r in c.execute(
            "SELECT biz_date, partition_key, COUNT(*) c FROM recut_runs WHERE status=?"
            " GROUP BY biz_date, partition_key", (RESULT_CONTENTION,))}
        locks = [dict(r) for r in c.execute("SELECT * FROM partition_locks ORDER BY id")]
    return {"policy": PINNED_POLICY, "note": POLICY_NOTE, "items": runs,
            "contention": [{"biz_date": k[0], "partition_key": k[1], "count": v}
                           for k, v in contention.items()],
            "locks": locks}


def _partition_groups(conn: sqlite3.Connection) -> list[dict]:
    slices = [dict(r) for r in conn.execute("SELECT * FROM slices ORDER BY biz_date, partition_key, id")]
    ledger = {r["biz_date"]: dict(r) for r in conn.execute("SELECT * FROM recut_ledger")}
    locks = {(r["biz_date"], r["partition_key"]): dict(r) for r in conn.execute("SELECT * FROM partition_locks")}
    runs = [dict(r) for r in conn.execute("SELECT * FROM recut_runs ORDER BY id DESC")]
    watermarks = [dict(r) for r in conn.execute("SELECT * FROM watermarks ORDER BY id DESC")]

    groups: dict[tuple[str, str], dict] = {}
    for s in slices:
        key = (s["biz_date"], s["partition_key"])
        g = groups.setdefault(key, {"biz_date": key[0], "partition_key": key[1], "slices": [],
                                    "active_slices": [], "retired_slices": [],
                                    "active": 0, "retired": 0, "released": 0, "complete": 0,
                                    "retired_released": 0, "policy": None, "policy_note": POLICY_NOTE,
                                    "lock": None, "last_run": None, "contention": 0,
                                    "skip_count": 0, "skip_note": ""})
        g["slices"].append(s)
        if s["retired_at"]:
            g["retired"] += 1
            g["retired_slices"].append(s)
            if s["release_status"] == "已撤回":
                g["retired_released"] += 1
        else:
            g["active"] += 1
            g["active_slices"].append(s)
            if s["release_status"] == "已放行":
                g["released"] += 1
            if s["completeness"] == "完整":
                g["complete"] += 1
    # 有锁或有重切运行、但尚无切片的分区也要在队列/详情里出现
    for key in list(locks) + [(r["biz_date"], r["partition_key"]) for r in runs]:
        groups.setdefault(key, {"biz_date": key[0], "partition_key": key[1], "slices": [],
                                "active_slices": [], "retired_slices": [],
                                "active": 0, "retired": 0, "released": 0, "complete": 0,
                                "retired_released": 0, "policy": None, "policy_note": POLICY_NOTE,
                                "lock": None, "last_run": None, "contention": 0,
                                "skip_count": 0, "skip_note": ""})
    now = now_iso()
    for key, g in groups.items():
        ledger_row = ledger.get(key[0])
        g["policy"] = ledger_row["policy"] if ledger_row else PINNED_POLICY
        g["policy_pinned_at"] = ledger_row["created_at"] if ledger_row else None
        lk = locks.get(key)
        if lk and lk["expires_at"] >= now:
            g["lock"] = {"owner": lk["owner"], "run_no": lk["run_no"],
                         "acquired_at": lk["acquired_at"], "expires_at": lk["expires_at"]}
        for r in runs:
            if (r["biz_date"], r["partition_key"]) == key:
                if g["last_run"] is None:
                    g["last_run"] = {"run_no": r["run_no"], "status": r["status"],
                                     "created_at": r["created_at"], "finished_at": r["finished_at"],
                                     "note": r["note"], "requested_by": r["requested_by"]}
                if r["status"] == RESULT_CONTENTION:
                    g["contention"] += 1
        # 分区水位对齐：取每个消费方在该业务日的当前 head，仅当 head 来源属于本分区时展示
        active_released = {s["slice_no"] for s in g["active_slices"] if s["release_status"] == "已放行"}
        slice_by_no = {s["slice_no"]: s for s in slices}
        heads: dict[str, dict] = {}
        seen_consumer: set[str] = set()
        for w in watermarks:  # watermarks 已按 id DESC 排序
            if w["biz_date"] != key[0] or w["consumer"] in seen_consumer:
                continue
            seen_consumer.add(w["consumer"])
            src = slice_by_no.get(w["source_slice"])
            if src is None or src["partition_key"] != key[1]:
                continue
            aligned = (not src["retired_at"] and src["release_status"] == "已放行")
            heads[w["consumer"]] = {"consumer": w["consumer"], "watermark": w["watermark"],
                                    "source_slice": w["source_slice"],
                                    "aligned": aligned,
                                    "align": "一致" if aligned else (
                                        "来源已撤回" if src["retired_at"] else "来源未放行"),
                                    "active_released": sorted(active_released)}
        g["watermark_heads"] = list(heads.values())
        g["watermark_aligned"] = all(h["aligned"] for h in heads.values()) if heads else True
    return sorted(groups.values(), key=lambda x: (x["biz_date"], x["partition_key"]))


@app.get("/api/partitions")
def list_partitions(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        groups = _partition_groups(c)
    return {"policy": PINNED_POLICY, "label": POLICY_LABEL, "note": POLICY_NOTE, "items": groups}


@app.get("/api/recut/overview")
def recut_overview(limit: int = 50, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with closing(connect()) as c:
        groups = _partition_groups(c)
        runs = _recut_runs(c, None, None, limit)
        watermarks = _watermark_view(c)
        ledger = [dict(r) for r in c.execute("SELECT * FROM recut_ledger ORDER BY biz_date")]
        stats = {
            "rollbacks": c.execute("SELECT COUNT(*) c FROM recut_runs WHERE status=?",
                                   (RESULT_ROLLBACK,)).fetchone()["c"],
            "conflicts": c.execute("SELECT COUNT(*) c FROM recut_runs WHERE status=?",
                                   (RESULT_CONFLICT,)).fetchone()["c"],
            "contention": c.execute("SELECT COUNT(*) c FROM recut_runs WHERE status=?",
                                    (RESULT_CONTENTION,)).fetchone()["c"],
            "deviations": c.execute("SELECT COUNT(*) c FROM recut_runs WHERE status=?",
                                    (RESULT_DEVIATION,)).fetchone()["c"],
            "success": c.execute("SELECT COUNT(*) c FROM recut_runs WHERE status=?",
                                 (RESULT_SUCCESS,)).fetchone()["c"],
            "running": c.execute("SELECT COUNT(*) c FROM recut_runs WHERE status=?",
                                 (RESULT_RUNNING,)).fetchone()["c"],
            "runs": c.execute("SELECT COUNT(*) c FROM recut_runs").fetchone()["c"],
            "skip_count": 0,
            "skip_note": "口径甲不回填重切跳过次数（该计数属于未采用的方案乙）",
        }
        locks = [dict(r) for r in c.execute("SELECT * FROM partition_locks ORDER BY id")]
    return {"policy": {"pinned": PINNED_POLICY, "label": POLICY_LABEL, "note": POLICY_NOTE,
                       "not_adopted": "保留完整",
                       "skip_count_note": stats["skip_note"]},
            "ledger": ledger, "partitions": groups, "runs": runs,
            "watermarks": watermarks, "locks": locks, "stats": stats}


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
