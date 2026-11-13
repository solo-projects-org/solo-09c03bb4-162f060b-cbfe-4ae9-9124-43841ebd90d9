"""随访明细交付平台（单实例）

- 导入三列 CSV（participant_id, visit_date, score），新参与者默认已授权
- 按所选参与者生成批次交付 CSV（保留其全部随访行）
- 永久撤回授权：中止含此人的生成作业与在途下载，相关批次永久禁止下载
- 元数据（SQLite）与批次文件均落在数据卷，重启后撤回仍然生效
"""
from __future__ import annotations

import asyncio
import csv
import io
import os
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", str(BASE_DIR.parent / "data")))
DB_PATH = DATA_DIR / "app.db"
BATCH_DIR = DATA_DIR / "batches"
SAMPLE_CSV = BASE_DIR / "static" / "sample_followups.csv"

# 生成作业的节流（便于演示“生成中撤回”）
GEN_CHUNK_ROWS = int(os.environ.get("GEN_CHUNK_ROWS", "100"))
GEN_CHUNK_DELAY_MS = int(os.environ.get("GEN_CHUNK_DELAY_MS", "100"))
# 下载节流：默认全速；slow=1 时人为放慢，用于演示“慢下载期间撤回”
DL_CHUNK_BYTES = int(os.environ.get("DL_CHUNK_BYTES", "65536"))
DL_CHUNK_DELAY_MS = int(os.environ.get("DL_CHUNK_DELAY_MS", "0"))
SLOW_CHUNK_BYTES = int(os.environ.get("SLOW_CHUNK_BYTES", "2048"))
SLOW_CHUNK_DELAY_MS = int(os.environ.get("SLOW_CHUNK_DELAY_MS", "200"))

EXPECTED_COLS = ("participant_id", "visit_date", "score")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS participants(
              participant_id TEXT PRIMARY KEY,
              authorized     INTEGER NOT NULL DEFAULT 1,
              revoked_at     TEXT
            );
            CREATE TABLE IF NOT EXISTS visits(
              participant_id TEXT NOT NULL,
              visit_date     TEXT NOT NULL,
              score          TEXT,
              PRIMARY KEY(participant_id, visit_date)
            );
            CREATE TABLE IF NOT EXISTS batches(
              id          INTEGER PRIMARY KEY AUTOINCREMENT,
              status      TEXT NOT NULL,           -- generating|ready|failed|blocked
              created_at  TEXT NOT NULL,
              finished_at TEXT,
              file_path   TEXT,
              error       TEXT
            );
            CREATE TABLE IF NOT EXISTS batch_participants(
              batch_id       INTEGER NOT NULL,
              participant_id TEXT NOT NULL,
              PRIMARY KEY(batch_id, participant_id)
            );
            CREATE TABLE IF NOT EXISTS revocations(
              participant_id TEXT PRIMARY KEY,
              revoked_at     TEXT NOT NULL
            );
            """
        )
        # 重启恢复：上次未完成的生成批次一律标记失败，不提供下载
        conn.execute(
            "UPDATE batches SET status='failed', error='interrupted by restart', "
            "finished_at=? WHERE status='generating'",
            (utcnow(),),
        )
        # ready 但文件丢失的批次同样不可下载
        for row in conn.execute(
            "SELECT id, file_path FROM batches WHERE status='ready'"
        ).fetchall():
            if not row["file_path"] or not Path(row["file_path"]).exists():
                conn.execute(
                    "UPDATE batches SET status='failed', "
                    "error='batch file missing after restart' WHERE id=?",
                    (row["id"],),
                )


# ---------------------------------------------------------------- 运行时状态

class JobState:
    """一个生成作业：cancel 由撤回触发，done 在作业彻底停止后置位。"""

    def __init__(self) -> None:
        self.cancel = asyncio.Event()
        self.done = asyncio.Event()


class DownloadState:
    """一个在途下载：abort 由撤回触发，done 在流彻底停止后置位。"""

    def __init__(self, batch_id: int) -> None:
        self.batch_id = batch_id
        self.abort = asyncio.Event()
        self.done = asyncio.Event()


class AppState:
    def __init__(self) -> None:
        # 所有“检查状态 + 变更状态”的临界区都走这把锁，
        # 保证 生成完成 / 开始下载 / 撤回 三者并发时只会按一种顺序生效。
        self.lock = asyncio.Lock()
        self.jobs: dict[int, JobState] = {}
        self.downloads: dict[str, DownloadState] = {}


state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="随访明细交付平台", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(BASE_DIR / "static" / "index.html")


# ---------------------------------------------------------------- 数据导入

def import_csv_bytes(data: bytes) -> dict:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError(f"CSV 必须是 UTF-8 编码: {exc}")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV 为空或缺少表头")
    reader.fieldnames = [(h or "").strip().lower() for h in reader.fieldnames]
    missing = [c for c in EXPECTED_COLS if c not in reader.fieldnames]
    if missing:
        raise ValueError(f"缺少必需列: {', '.join(missing)}（需要 participant_id,visit_date,score）")

    participants: set[str] = set()
    rows: list[tuple[str, str, str]] = []
    for lineno, row in enumerate(reader, start=2):
        pid = (row.get("participant_id") or "").strip()
        visit_date = (row.get("visit_date") or "").strip()
        score = (row.get("score") or "").strip()
        if not pid or not visit_date:
            raise ValueError(f"第 {lineno} 行 participant_id / visit_date 不能为空")
        participants.add(pid)
        rows.append((pid, visit_date, score))
    if not rows:
        raise ValueError("CSV 没有数据行")

    with connect() as conn:
        # 新参与者默认已授权；已存在的（含已撤回的）保持原授权状态
        conn.executemany(
            "INSERT OR IGNORE INTO participants(participant_id, authorized) VALUES(?, 1)",
            [(p,) for p in participants],
        )
        conn.executemany(
            "INSERT OR REPLACE INTO visits(participant_id, visit_date, score) VALUES(?,?,?)",
            rows,
        )
    return {"participants": len(participants), "rows": len(rows)}


@app.post("/api/import")
async def import_csv(file: UploadFile = File(...)):
    data = await file.read()
    try:
        return await asyncio.to_thread(import_csv_bytes, data)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/import-sample")
def import_sample():
    try:
        return import_csv_bytes(SAMPLE_CSV.read_bytes())
    except ValueError as exc:
        raise HTTPException(400, str(exc))


# ---------------------------------------------------------------- 查询

@app.get("/api/participants")
def list_participants():
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT p.participant_id, p.authorized, p.revoked_at,
                   COUNT(v.visit_date) AS visits
            FROM participants p
            LEFT JOIN visits v ON v.participant_id = p.participant_id
            GROUP BY p.participant_id
            ORDER BY p.participant_id
            """
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/batches")
def list_batches():
    with connect() as conn:
        batches = conn.execute("SELECT * FROM batches ORDER BY id DESC").fetchall()
        parts = conn.execute(
            "SELECT batch_id, participant_id FROM batch_participants ORDER BY participant_id"
        ).fetchall()
    by_batch: dict[int, list[str]] = {}
    for p in parts:
        by_batch.setdefault(p["batch_id"], []).append(p["participant_id"])
    out = []
    for b in batches:
        d = dict(b)
        d["participants"] = by_batch.get(b["id"], [])
        out.append(d)
    return out


# ---------------------------------------------------------------- 批次生成

class BatchCreate(BaseModel):
    participant_ids: list[str]


class Cancelled(Exception):
    """生成作业被撤回打断。"""


def _fetch_rows(batch_id: int) -> list[tuple[str, str, str]]:
    with connect() as conn:
        cur = conn.execute(
            """
            SELECT v.participant_id, v.visit_date, v.score
            FROM visits v
            JOIN batch_participants bp ON bp.participant_id = v.participant_id
            WHERE bp.batch_id = ?
            ORDER BY v.participant_id, v.visit_date
            """,
            (batch_id,),
        )
        return [(r["participant_id"], r["visit_date"], r["score"]) for r in cur.fetchall()]


async def run_generation(batch_id: int, st: JobState) -> None:
    tmp_path = BATCH_DIR / f"batch_{batch_id}.csv.tmp"
    final_path = BATCH_DIR / f"batch_{batch_id}.csv"
    try:
        rows = await asyncio.to_thread(_fetch_rows, batch_id)
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(EXPECTED_COLS)
            for i in range(0, len(rows), GEN_CHUNK_ROWS):
                if st.cancel.is_set():
                    raise Cancelled()
                for r in rows[i : i + GEN_CHUNK_ROWS]:
                    writer.writerow(r)
                f.flush()
                if GEN_CHUNK_DELAY_MS > 0:
                    # 可被打断的睡眠：撤回时立即醒来，不拖延
                    try:
                        await asyncio.wait_for(st.cancel.wait(), GEN_CHUNK_DELAY_MS / 1000)
                        raise Cancelled()
                    except asyncio.TimeoutError:
                        pass
        # 落盘与“撤回置 blocked”之间的竞态在此临界区内裁决：
        # 要么撤回先置 blocked（本作业丢弃文件），要么本作业先置 ready（撤回随后封锁并清理）。
        async with state.lock:
            with connect() as conn:
                cur = conn.execute(
                    "SELECT status FROM batches WHERE id=?", (batch_id,)
                ).fetchone()
                if st.cancel.is_set() or (cur and cur["status"] == "blocked"):
                    raise Cancelled()
                os.replace(tmp_path, final_path)
                conn.execute(
                    "UPDATE batches SET status='ready', finished_at=?, file_path=? WHERE id=?",
                    (utcnow(), str(final_path), batch_id),
                )
    except Cancelled:
        tmp_path.unlink(missing_ok=True)
        # 批次状态已由撤回流程置为 blocked，这里不再改动
    except Exception as exc:  # 生成/保存失败：批次置 failed，不提供下载
        tmp_path.unlink(missing_ok=True)
        with connect() as conn:
            conn.execute(
                "UPDATE batches SET status='failed', error=?, finished_at=? "
                "WHERE id=? AND status='generating'",
                (str(exc), utcnow(), batch_id),
            )
    finally:
        st.done.set()
        state.jobs.pop(batch_id, None)


@app.post("/api/batches", status_code=201)
async def create_batch(body: BatchCreate):
    ids = list(dict.fromkeys(body.participant_ids))  # 去重保序
    if not ids:
        raise HTTPException(400, "未选择任何参与者")
    async with state.lock:
        with connect() as conn:
            placeholders = ",".join("?" for _ in ids)
            rows = conn.execute(
                f"SELECT participant_id, authorized FROM participants "
                f"WHERE participant_id IN ({placeholders})",
                ids,
            ).fetchall()
            found = {r["participant_id"]: r["authorized"] for r in rows}
            unknown = [p for p in ids if p not in found]
            if unknown:
                raise HTTPException(404, f"参与者不存在: {', '.join(unknown)}")
            revoked = [p for p in ids if not found[p]]
            if revoked:
                # 选到已撤回人员：拒绝创建
                raise HTTPException(409, f"以下参与者已撤回授权，拒绝创建批次: {', '.join(revoked)}")
            cur = conn.execute(
                "INSERT INTO batches(status, created_at) VALUES('generating', ?)",
                (utcnow(),),
            )
            batch_id = cur.lastrowid
            # 创建时快照所选人员（此刻均已授权）
            conn.executemany(
                "INSERT INTO batch_participants(batch_id, participant_id) VALUES(?,?)",
                [(batch_id, p) for p in ids],
            )
        st = JobState()
        state.jobs[batch_id] = st
    asyncio.create_task(run_generation(batch_id, st))
    return {"batch_id": batch_id, "status": "generating", "participants": ids}


# ---------------------------------------------------------------- 下载

@app.get("/api/batches/{batch_id}/download")
async def download_batch(batch_id: int, slow: bool = False):
    dl_id = uuid.uuid4().hex
    # 临界区：确认可下载并登记在途下载。
    # 与撤回并发时，要么本下载先登记（随后被撤回中止），要么撤回先封锁（此处被拒绝）。
    async with state.lock:
        with connect() as conn:
            b = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if b is None:
            raise HTTPException(404, "批次不存在")
        if b["status"] == "blocked":
            raise HTTPException(403, "该批次包含已撤回授权的参与者，已永久禁止下载")
        if b["status"] != "ready":
            raise HTTPException(409, f"批次不可下载（当前状态: {b['status']}）")
        path = b["file_path"]
        if not path or not Path(path).exists():
            raise HTTPException(410, "批次文件不可用")
        dst = DownloadState(batch_id)
        state.downloads[dl_id] = dst

    chunk = SLOW_CHUNK_BYTES if slow else DL_CHUNK_BYTES
    delay = (SLOW_CHUNK_DELAY_MS if slow else DL_CHUNK_DELAY_MS) / 1000

    async def stream():
        try:
            with open(path, "rb") as f:
                while True:
                    if dst.abort.is_set():
                        return  # 被撤回：停止发送（已发出的字节无需追回）
                    data = f.read(chunk)
                    if not data:
                        return
                    yield data
                    if delay > 0:
                        try:
                            await asyncio.wait_for(dst.abort.wait(), delay)
                            return  # 睡眠期间被撤回，立即停止
                        except asyncio.TimeoutError:
                            pass
        finally:
            dst.done.set()
            state.downloads.pop(dl_id, None)

    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="batch_{batch_id}.csv"'},
    )


# ---------------------------------------------------------------- 撤回授权

@app.post("/api/participants/{pid}/revoke")
async def revoke_participant(pid: str):
    async with state.lock:
        with connect() as conn:
            p = conn.execute(
                "SELECT * FROM participants WHERE participant_id=?", (pid,)
            ).fetchone()
            if p is None:
                raise HTTPException(404, "参与者不存在")
            if not p["authorized"]:
                raise HTTPException(409, "该参与者授权已被撤回")
            now = utcnow()
            # 永久撤回 + 留存撤回记录（落数据卷，重启后仍生效）
            conn.execute(
                "UPDATE participants SET authorized=0, revoked_at=? WHERE participant_id=?",
                (now, pid),
            )
            conn.execute(
                "INSERT INTO revocations(participant_id, revoked_at) VALUES(?,?)",
                (pid, now),
            )
            affected = [
                r["id"]
                for r in conn.execute(
                    "SELECT DISTINCT b.id FROM batches b "
                    "JOIN batch_participants bp ON bp.batch_id = b.id "
                    "WHERE bp.participant_id = ? AND b.status IN ('generating','ready')",
                    (pid,),
                ).fetchall()
            ]
            if affected:
                placeholders = ",".join("?" for _ in affected)
                conn.execute(
                    f"UPDATE batches SET status='blocked', finished_at=? "
                    f"WHERE id IN ({placeholders})",
                    [now, *affected],
                )
        # 中止含此人的生成作业与在途下载（只动受影响的，其他批次照常）
        jobs: list[JobState] = []
        for bid in affected:
            job = state.jobs.get(bid)
            if job is not None:
                job.cancel.set()
                jobs.append(job)
        downloads: list[DownloadState] = []
        for d in state.downloads.values():
            if d.batch_id in affected:
                d.abort.set()
                downloads.append(d)

    # 锁外等待：相关作业和下载都真正停止后才报告撤回成功。
    # 只等受影响的作业/下载，慢下载不会卡住不相关批次。
    for job in jobs:
        await job.done.wait()
    for d in downloads:
        await d.done.wait()

    return {
        "participant_id": pid,
        "revoked": True,
        "blocked_batches": affected,
        "aborted_jobs": len(jobs),
        "aborted_downloads": len(downloads),
    }
