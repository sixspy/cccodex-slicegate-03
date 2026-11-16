
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

import app as A
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(c, username="admin", password="123456"):
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _recut(c, h, biz, part, slices, consumer="ads_order", policy=None):
    body = {"biz_date": biz, "partition_key": part, "consumer": consumer, "slices": slices}
    if policy is not None:
        body["policy"] = policy
    return c.post("/api/recut", headers=h, json=body)


def _slices(c, h):
    return c.get("/api/slices", headers=h).json()["items"]


def _wm(c, h):
    return c.get("/api/watermarks", headers=h).json()["items"]


def _parts(c, h):
    return {(p["biz_date"], p["partition_key"]): p for p in c.get("/api/partitions", headers=h).json()["items"]}


# ---------------------------------------------------------------------------
# 基础链路
# ---------------------------------------------------------------------------
def test_pipeline():
    c = TestClient(app)
    assert c.get("/api/health").json()["product"] == "日切放行"
    h = _login(c)
    assert len(_slices(c, h)) >= 1
    bad = c.post("/api/slices/1/release", headers=h, json={"consumer": "x"})
    assert bad.status_code == 409
    ok = c.post("/api/slices", headers=h, json={"slice_no": "SL-T-1", "biz_date": "2026-11-04",
                                               "partition_key": "mid", "row_count": 10, "expected_count": 10})
    assert ok.status_code == 200
    sid = ok.json()["id"]
    rel = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_x"})
    assert rel.status_code == 200
    assert any(w["source_slice"] == "SL-T-1" for w in _wm(c, h))


# ---------------------------------------------------------------------------
# 重切成功：旧切片整片撤回，只有一套正式放行；水位只指向完整已放行切片
# ---------------------------------------------------------------------------
def test_recut_success_retires_old_release_and_moves_watermark():
    c = TestClient(app)
    h = _login(c)
    r = _recut(c, h, "2026-11-01", "west",
               [{"slice_no": "SL-W2-a", "row_count": 1000, "expected_count": 1000},
                {"slice_no": "SL-W2-b", "row_count": 1200, "expected_count": 1000}])
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["policy"] == A.PINNED_POLICY
    assert j["run"]["status"] == "成功"

    items = {s["slice_no"]: s for s in _slices(c, h)}
    assert items["SL-2026-11-01-west"]["release_status"] == "已撤回"
    assert items["SL-2026-11-01-west"]["retired_at"]
    # 该分区在役切片只剩新切片，旧切片不得再是正式放行
    active = [s for s in items.values()
              if s["active"] and (s["biz_date"], s["partition_key"]) == ("2026-11-01", "west")]
    assert {s["slice_no"] for s in active} == {"SL-W2-a", "SL-W2-b"}
    assert all(s["release_status"] == "已放行" for s in active)

    heads = [w for w in _wm(c, h) if w["is_head"]]
    assert len(heads) == 1
    assert heads[0]["source_slice"] == "SL-W2-b"
    assert heads[0]["align"] == "一致"
    assert heads[0]["source_completeness"] == "完整"
    # 旧水位留档但不再是 head
    old = [w for w in _wm(c, h) if w["source_slice"] == "SL-2026-11-01-west"]
    assert old and not old[0]["is_head"] and old[0]["align"] == "来源已撤回"

    part = _parts(c, h)[("2026-11-01", "west")]
    assert part["active"] == 2 and part["retired"] == 1
    assert part["released"] == 2 and part["watermark_aligned"] is True


# ---------------------------------------------------------------------------
# 重切失败（未齐）：整笔回滚，旧放行态与水位保持重切前快照
# ---------------------------------------------------------------------------
def test_recut_partial_rolls_back_and_keeps_snapshot():
    c = TestClient(app)
    h = _login(c)
    before_slices = _slices(c, h)
    before_wm = _wm(c, h)
    r = _recut(c, h, "2026-11-01", "west",
               [{"slice_no": "SL-W3-ok", "row_count": 1000, "expected_count": 1000},
                {"slice_no": "SL-W3-bad", "row_count": 5, "expected_count": 1000}])
    assert r.status_code == 409
    j = r.json()
    assert j["error"] == "失败回滚"
    assert j["run"]["result"]["snapshot_intact"] is True
    statuses = {i["slice_no"]: i["status"] for i in j["run"]["items"]}
    assert statuses["SL-W3-ok"] == "未保留"
    assert statuses["SL-W3-bad"] == "未齐"

    assert _slices(c, h) == before_slices
    assert _wm(c, h) == before_wm
    # 失败不得推进水位，也不得留半写切片
    assert not any(s["slice_no"].startswith("SL-W3") for s in _slices(c, h))
    part = _parts(c, h)[("2026-11-01", "west")]
    assert part["last_run"]["status"] == "失败回滚"
    assert part["watermark_aligned"] is True


# ---------------------------------------------------------------------------
# 同业务日同分区分片号冲突：整次有失败闭，不落任何切片
# ---------------------------------------------------------------------------
def test_recut_same_partition_slice_no_conflict_fails_whole_run():
    c = TestClient(app)
    h = _login(c)
    before = _slices(c, h)
    r = _recut(c, h, "2026-11-01", "east",
               [{"slice_no": "SL-2026-11-01-east", "row_count": 1000, "expected_count": 1000}])
    assert r.status_code == 409
    j = r.json()
    assert j["error"] == "切片号冲突"
    assert "同业务日同分区分片号冲突" in j["run"]["items"][0]["reason"]
    assert j["run"]["result"]["snapshot_intact"] is True
    assert _slices(c, h) == before

    # 同趟内重复切片号同样整次失败
    r = _recut(c, h, "2026-11-01", "east",
               [{"slice_no": "SL-DUP", "row_count": 1000, "expected_count": 1000},
                {"slice_no": "SL-DUP", "row_count": 1000, "expected_count": 1000}])
    assert r.status_code == 409 and r.json()["error"] == "切片号冲突"
    assert not any(s["slice_no"] == "SL-DUP" for s in _slices(c, h))


# ---------------------------------------------------------------------------
# 口径偏离：裁决簿钉死甲案后，请求乙案被拒绝并记「重切口径偏了」
# ---------------------------------------------------------------------------
def test_policy_deviation_rejected_and_logged_without_writes():
    c = TestClient(app)
    h = _login(c)
    # 首次以甲案钉死裁决簿
    ok = _recut(c, h, "2026-11-01", "west",
                [{"slice_no": "SL-W4-a", "row_count": 1000, "expected_count": 1000}],
                policy="整笔回滚")
    assert ok.status_code == 200
    ledger = c.get("/api/recut/ledger", headers=h).json()["items"]
    assert [l["policy"] for l in ledger if l["biz_date"] == "2026-11-01"] == ["整笔回滚"]

    before_slices = _slices(c, h)
    before_wm = _wm(c, h)
    r = _recut(c, h, "2026-11-01", "west",
               [{"slice_no": "SL-W4-b", "row_count": 1000, "expected_count": 1000}],
               policy="保留完整")
    assert r.status_code == 409
    j = r.json()
    assert j["error"] == "口径偏离"
    assert j["pinned_policy"] == "整笔回滚" and j["wanted_policy"] == "保留完整"
    assert "重切口径偏了" in j["run"]["note"]
    # 未写入任何切片/水位，裁决簿当日仍为甲案
    assert _slices(c, h) == before_slices
    assert _wm(c, h) == before_wm
    ledger = c.get("/api/recut/ledger", headers=h).json()["items"]
    assert [l["policy"] for l in ledger if l["biz_date"] == "2026-11-01"] == ["整笔回滚"]


def test_ledger_pinned_per_biz_date():
    c = TestClient(app)
    h = _login(c)
    # 不同业务日可各自钉死，但同一日内不得更换；默认钉死甲案
    assert _recut(c, h, "2026-11-06", "a",
                  [{"slice_no": "SL-06-a", "row_count": 10, "expected_count": 10}]).status_code == 200
    assert _recut(c, h, "2026-11-07", "a",
                  [{"slice_no": "SL-07-a", "row_count": 10, "expected_count": 10}]).status_code == 200
    ledger = {l["biz_date"]: l["policy"] for l in c.get("/api/recut/ledger", headers=h).json()["items"]}
    assert ledger == {"2026-11-06": "整笔回滚", "2026-11-07": "整笔回滚"}


# ---------------------------------------------------------------------------
# 同分区并发重切：加锁互斥，后到者记「重切争用」并留痕
# ---------------------------------------------------------------------------
def test_concurrent_recut_records_contention():
    c = TestClient(app)
    h = _login(c)
    viewer = _login(c, "viewer", "view123")
    results = []

    def fire(token, name):
        client = TestClient(app)
        r = client.post("/api/recut", headers=token, json={
            "biz_date": "2026-11-05", "partition_key": "north", "consumer": "ads_order",
            "slices": [{"slice_no": f"SL-N-{name}", "row_count": 100, "expected_count": 100}]})
        results.append((name, r.status_code, r.json()))

    threads = [threading.Thread(target=fire, args=(h, "a")),
               threading.Thread(target=fire, args=(viewer, "b"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 后到者被互斥挡住（409）或排队后成功，但绝不静默通过：必须有重切争用留痕
    assert any(code in (200, 409) for _, code, _ in results), results
    assert any(code == 200 for _, code, _ in results), results
    runs = c.get("/api/recut/runs", headers=h).json()["items"]
    contention = [r for r in runs if r["status"] == "重切争用"]
    assert contention, runs
    assert "未静默通过" in contention[0]["note"]
    part = _parts(c, h)[("2026-11-05", "north")]
    assert part["contention"] == 1
    assert part["last_run"]["status"] in ("成功", "重切争用")
    # 最终只保留一套在役切片
    active = [s for s in _slices(c, h) if s["active"] and s["partition_key"] == "north"]
    assert len(active) == 1


def test_lock_holder_blocks_and_records_contention():
    c = TestClient(app)
    h = _login(c)
    A.LOCK_WAIT_SECONDS = 0.3
    try:
        conn = A.connect()
        from datetime import datetime, timedelta
        conn.execute(
            "INSERT INTO partition_locks(biz_date,partition_key,owner,token,run_no,acquired_at,expires_at)"
            " VALUES (?,?,?,?,?,?,?)",
            ("2026-11-09", "east", "viewer", "tok-x", "RC-other", A.now_iso(),
             (datetime.now() + timedelta(minutes=5)).isoformat(timespec="milliseconds")))
        conn.commit()
        conn.close()
        r = _recut(c, h, "2026-11-09", "east",
                   [{"slice_no": "SL-09-a", "row_count": 10, "expected_count": 10}])
        assert r.status_code == 409
        j = r.json()
        assert j["error"] == "重切争用"
        assert j["holder"]["owner"] == "viewer"
        # 未静默通过：没有任何切片写入
        assert not any(s["slice_no"] == "SL-09-a" for s in _slices(c, h))
        part = _parts(c, h)[("2026-11-09", "east")]
        assert part["lock"]["owner"] == "viewer"
        assert part["contention"] >= 1
    finally:
        A.LOCK_WAIT_SECONDS = 20


# ---------------------------------------------------------------------------
# 三个画面口径一致：队列、分区详情、水位
# ---------------------------------------------------------------------------
def test_queue_detail_watermark_surfaces_agree():
    c = TestClient(app)
    h = _login(c)
    assert _recut(c, h, "2026-11-01", "west",
                  [{"slice_no": "SL-W5-a", "row_count": 1000, "expected_count": 1000}]).status_code == 200
    bad = _recut(c, h, "2026-11-08", "east",
                 [{"slice_no": "SL-08-a", "row_count": 1, "expected_count": 100}])
    assert bad.status_code == 409

    runs = c.get("/api/recut/runs", headers=h).json()["items"]
    parts = _parts(c, h)
    wms = _wm(c, h)

    # 队列里的每个运行，都能在对应分区详情的 last_run 或争用计数上找到
    for r in runs:
        p = parts[(r["biz_date"], r["partition_key"])]
        if r["status"] == "重切争用":
            assert p["contention"] >= 1
        elif p["last_run"]["run_no"] == r["run_no"]:
            assert p["last_run"]["status"] == r["status"]

    # 水位 head 必须指向在役已放行的完整切片，且分区详情一致
    for w in [x for x in wms if x["is_head"]]:
        assert w["align"] == "一致"
        assert w["source_completeness"] == "完整"
        assert w["source_release_status"] == "已放行"
    for (biz, part), p in parts.items():
        for head in p["watermark_heads"]:
            assert head["aligned"] is True or head["align"] != "一致"

    # 重切成功后不得存在两套正式放行
    active_released = [s for s in _slices(c, h)
                       if s["active"] and s["release_status"] == "已放行"
                       and (s["biz_date"], s["partition_key"]) == ("2026-11-01", "west")]
    assert len(active_released) == 1
    assert [s["slice_no"] for s in active_released] == ["SL-W5-a"]


def test_policy_endpoint_annotates_pinned_choice():
    c = TestClient(app)
    h = _login(c)
    j = c.get("/api/recut/policies", headers=h).json()
    assert j["pinned"] == "整笔回滚"
    assert "整笔回滚" in j["note"] and "保留完整" in j["not_adopted"]
    assert "跳过" in j["skip_count_note"]
    ov = c.get("/api/recut/overview", headers=h).json()
    assert ov["policy"]["pinned"] == "整笔回滚"
    assert ov["stats"]["skip_count"] == 0
