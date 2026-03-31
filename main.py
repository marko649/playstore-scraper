"""
Play Store Scraper — FastAPI Backend (Render Edition)
=====================================================
Render injects a $PORT environment variable at runtime.
The start command in render.yaml passes it to uvicorn automatically.
"""

import asyncio
import io
import json
import logging
import queue
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from scraper import run_scrape_job

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Play Store Scraper", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory job store ───────────────────────────────────────────────────────
_jobs: dict = {}
_jobs_lock = threading.Lock()
REQUIRED_COLUMNS = {"Company Name", "Website"}


def _make_job(job_id: str, total: int, df_input: pd.DataFrame) -> dict:
    return {
        "id": job_id,
        "status": "queued",
        "total": total,
        "processed": 0,
        "matched": 0,
        "no_app": 0,
        "failed": 0,
        "progress_queue": queue.Queue(),
        "df_input": df_input,
        "df_result": None,
        "error": None,
        "started_at": time.time(),
        "finished_at": None,
    }


# ── Background worker ─────────────────────────────────────────────────────────

def _run_job(job_id: str) -> None:
    job = _jobs.get(job_id)
    if not job:
        return

    job["status"] = "running"
    log.info("Job %s started (%d rows)", job_id, job["total"])

    def on_progress(event: dict) -> None:
        job["progress_queue"].put(event)
        if event.get("type") == "row":
            job["processed"] += 1
            s = event.get("status", "")
            if s == "matched":
                job["matched"] += 1
            elif s == "no_app":
                job["no_app"] += 1
            else:
                job["failed"] += 1

    try:
        df_result = run_scrape_job(job["df_input"].copy(), on_progress)
        job["df_result"] = df_result
        job["status"] = "done"
        job["finished_at"] = time.time()
        elapsed = round(job["finished_at"] - job["started_at"], 1)
        log.info("Job %s done in %.1fs", job_id, elapsed)
        job["progress_queue"].put({
            "type": "done",
            "matched": job["matched"],
            "no_app": job["no_app"],
            "failed": job["failed"],
            "elapsed": elapsed,
        })
    except Exception as exc:
        log.exception("Job %s crashed: %s", job_id, exc)
        job["status"] = "error"
        job["error"] = str(exc)
        job["finished_at"] = time.time()
        job["progress_queue"].put({"type": "error", "message": str(exc)})


# ── API Endpoints ─────────────────────────────────────────────────────────────

@app.post("/api/jobs")
async def create_job(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Only .csv files are accepted.")

    contents = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(contents), dtype=str).fillna("")
        df.columns = [c.strip() for c in df.columns]
    except Exception as exc:
        raise HTTPException(400, f"Could not parse CSV: {exc}")

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise HTTPException(400, f"CSV missing required columns: {', '.join(sorted(missing))}")

    if len(df) == 0:
        raise HTTPException(400, "CSV contains no data rows.")

    job_id = str(uuid.uuid4())
    job = _make_job(job_id, len(df), df)

    with _jobs_lock:
        _jobs[job_id] = job

    thread = threading.Thread(target=_run_job, args=(job_id,), daemon=True)
    thread.start()

    log.info("Job %s created — %d rows", job_id, len(df))
    return {"job_id": job_id, "total": len(df), "filename": file.filename}


@app.get("/api/jobs/{job_id}/status")
async def get_status(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    elapsed = round(time.time() - job["started_at"], 1)
    pct = round(job["processed"] / job["total"] * 100, 1) if job["total"] else 0
    eta = None
    if job["processed"] > 0 and job["status"] == "running" and elapsed > 0:
        rate = job["processed"] / elapsed
        remaining = job["total"] - job["processed"]
        eta = round(remaining / rate) if rate > 0 else None

    return {
        "id": job_id,
        "status": job["status"],
        "total": job["total"],
        "processed": job["processed"],
        "matched": job["matched"],
        "no_app": job["no_app"],
        "failed": job["failed"],
        "percent": pct,
        "elapsed_seconds": elapsed,
        "eta_seconds": eta,
        "error": job.get("error"),
    }


@app.get("/api/jobs/{job_id}/progress")
async def stream_progress(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    pq: queue.Queue = job["progress_queue"]

    async def event_generator():
        yield f"data: {json.dumps({'type': 'init', 'total': job['total']})}\n\n"
        while True:
            events = []
            try:
                while True:
                    events.append(pq.get_nowait())
            except queue.Empty:
                pass

            for event in events:
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("done", "error"):
                    return

            if not events:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"
                if job["status"] in ("done", "error") and pq.empty():
                    return

            await asyncio.sleep(0.25)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/api/jobs/{job_id}/results")
async def get_results(
    job_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    search: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    match_status: Optional[str] = Query(None),
):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] == "queued":
        raise HTTPException(400, "Job has not started yet.")

    df = job["df_result"] if job["df_result"] is not None else job["df_input"]
    if df is None or len(df) == 0:
        return {"total": 0, "page": 1, "pages": 0, "page_size": page_size,
                "categories": [], "rows": []}

    mask = pd.Series([True] * len(df), index=df.index)

    if search:
        s = search.lower()
        sub_mask = pd.Series([False] * len(df), index=df.index)
        for col in ["Company Name", "App Name", "Website"]:
            if col in df.columns:
                sub_mask |= df[col].str.lower().str.contains(s, na=False)
        mask &= sub_mask

    if category and "Category" in df.columns:
        mask &= df["Category"].str.lower() == category.lower()

    if match_status == "matched" and "App Name" in df.columns:
        mask &= (df["App Name"] != "No App") & (df["App Name"].str.strip() != "")
    elif match_status == "no_app" and "App Name" in df.columns:
        mask &= df["App Name"] == "No App"

    filtered = df[mask]
    total_filtered = len(filtered)
    start = (page - 1) * page_size
    page_df = filtered.iloc[start: start + page_size]

    categories = []
    if "Category" in df.columns:
        cats = df["Category"].dropna().unique().tolist()
        categories = sorted([c for c in cats if c and c not in ("No App", "N/A", "")])

    return {
        "total": total_filtered,
        "page": page,
        "pages": max(1, (total_filtered + page_size - 1) // page_size),
        "page_size": page_size,
        "categories": categories,
        "rows": page_df.to_dict(orient="records"),
    }


@app.get("/api/jobs/{job_id}/download")
async def download_results(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["df_result"] is None:
        raise HTTPException(400, "Results not ready yet.")

    buf = io.StringIO()
    job["df_result"].to_csv(buf, index=False)
    buf.seek(0)
    filename = f"playstore_enriched_{job_id[:8]}.csv"
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Serve React SPA ───────────────────────────────────────────────────────────
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
