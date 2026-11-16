
import sqlite3

import pytest
from fastapi.testclient import TestClient

import app as A
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(c):
    r = c.post("/api/login", json={"username": "admin", "password": "123456"})
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _recut(c, h, biz, part, slices):
    return c.post("/api/recut", headers=h, json={
        "biz_date": biz, "partition_key": part, "consumer": "ads_order", "slices": slices})


def test_forced_db_error_mid_write_rolls_back(monkeypatch):
    c = TestClient(app)
    h = _login(c)
    before_slices = c.get("/api/slices", headers=h).json()["items"]
    before_wm = c.get("/api/watermarks", headers=h).json()["items"]

    real_connect = A.connect

    class Boom(sqlite3.Connection):
        pass

    # 在成功落库的 INSERT 阶段制造异常：必须整笔回滚且水位不动
    original_insert_item = A._insert_item
    calls = {"n": 0}

    def flaky_insert(conn, run_id, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] >= 1:
            raise RuntimeError("模拟落库中途异常")
        return original_insert_item(conn, run_id, *args, **kwargs)

    monkeypatch.setattr(A, "_insert_item", flaky_insert)
    r = _recut(c, h, "2026-11-01", "west",
               [{"slice_no": "SL-BOOM-a", "row_count": 1000, "expected_count": 1000},
                {"slice_no": "SL-BOOM-b", "row_count": 1000, "expected_count": 1000}])
    assert r.status_code == 500
    j = r.json()
    assert j["error"] == "失败回滚"
    assert j["snapshot_intact"] is True
    assert c.get("/api/slices", headers=h).json()["items"] == before_slices
    assert c.get("/api/watermarks", headers=h).json()["items"] == before_wm
    assert not any(s["slice_no"].startswith("SL-BOOM") for s in c.get("/api/slices", headers=h).json()["items"])


def test_running_run_visible_before_commit_and_watermark_not_advanced(monkeypatch):
    c = TestClient(app)
    h = _login(c)
    observed = {}
    real_snapshot = A._partition_snapshot

    def spy(conn, biz_date, partition_key):
        snap = real_snapshot(conn, biz_date, partition_key)
        observed["watermarks_at_start"] = conn.execute(
            "SELECT COUNT(*) c FROM watermarks").fetchone()["c"]
        return snap

    monkeypatch.setattr(A, "_partition_snapshot", spy)
    calls = {"n": 0}

    def stop_after_running(conn, run_id, *args, **kwargs):
        calls["n"] += 1
        raise RuntimeError("中断在写入阶段")

    monkeypatch.setattr(A, "_insert_item", stop_after_running)
    r = _recut(c, h, "2026-11-01", "west",
               [{"slice_no": "SL-RUN-a", "row_count": 1000, "expected_count": 1000}])
    assert r.status_code == 500
    runs = c.get("/api/recut/runs", headers=h).json()["items"]
    run = [x for x in runs if x["status"] == "失败回滚"][0]
    # 进行中的运行先落库再执行，快照存在；失败后按甲案收口
    assert run["snapshot"]["taken_at"]
    assert run["result"]["snapshot_intact"] is True
    assert observed["watermarks_at_start"] == 1
    # 水位没有前进
    assert c.get("/api/watermarks", headers=h).json()["items"][0]["source_slice"] == "SL-2026-11-01-west"


def test_retired_slice_cannot_release_or_reevaluate():
    c = TestClient(app)
    h = _login(c)
    ok = _recut(c, h, "2026-11-01", "west",
                [{"slice_no": "SL-RET-a", "row_count": 1000, "expected_count": 1000}])
    assert ok.status_code == 200
    old = [s for s in c.get("/api/slices", headers=h).json()["items"]
           if s["slice_no"] == "SL-2026-11-01-west"][0]
    assert c.post(f"/api/slices/{old['id']}/release", headers=h,
                  json={"consumer": "ads_order"}).status_code == 409
    assert c.post(f"/api/slices/{old['id']}/evaluate", headers=h).status_code == 409


def test_successful_recut_has_single_formal_release_set():
    c = TestClient(app)
    h = _login(c)
    assert _recut(c, h, "2026-11-01", "west",
                  [{"slice_no": "SL-S1", "row_count": 1000, "expected_count": 1000},
                   {"slice_no": "SL-S2", "row_count": 1000, "expected_count": 1000}]).status_code == 200
    assert _recut(c, h, "2026-11-01", "west",
                  [{"slice_no": "SL-S3", "row_count": 1000, "expected_count": 1000}]).status_code == 200
    items = c.get("/api/slices", headers=h).json()["items"]
    west = [s for s in items if (s["biz_date"], s["partition_key"]) == ("2026-11-01", "west")]
    active = [s for s in west if s["active"]]
    assert [s["slice_no"] for s in active] == ["SL-S3"]
    assert active[0]["release_status"] == "已放行"
    assert all(s["release_status"] in ("已撤回", "已归档") for s in west if not s["active"])
    # 水位 head 只有一条，且指向最终在役切片
    heads = [w for w in c.get("/api/watermarks", headers=h).json()["items"] if w["is_head"]]
    assert len(heads) == 1 and heads[0]["source_slice"] == "SL-S3"


def test_release_blocked_while_partition_recut_in_flight():
    """重切进行中不得放行同分区切片，避免新旧两套正式放行并存。"""
    import datetime as _dt

    c = TestClient(app)
    h = _login(c)
    sid = [s for s in c.get("/api/slices", headers=h).json()["items"]
           if s["slice_no"] == "SL-2026-11-02-east"][0]["id"]
    A.DB_PATH  # 同一测试库
    conn = A.connect()
    conn.execute("UPDATE slices SET row_count=1000 WHERE id=?", (sid,))
    conn.execute(
        "INSERT INTO partition_locks(biz_date,partition_key,owner,token,run_no,acquired_at,expires_at)"
        " VALUES (?,?,?,?,?,?,?)",
        ("2026-11-02", "east", "viewer", "tok-y", "RC-live", A.now_iso(),
         (_dt.datetime.now() + _dt.timedelta(minutes=5)).isoformat(timespec="milliseconds")))
    conn.commit()
    conn.close()

    r = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_order"})
    assert r.status_code == 409
    assert "重切" in r.json()["detail"]
    item = [s for s in c.get("/api/slices", headers=h).json()["items"] if s["id"] == sid][0]
    assert item["release_status"] == "未放行"
    assert not any(w["source_slice"] == "SL-2026-11-02-east"
                   for w in c.get("/api/watermarks", headers=h).json()["items"])
