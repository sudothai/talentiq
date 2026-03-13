from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

from db import init_db
from ingest import ingest_resume
from search import search_candidates, list_candidates, chat_with_candidates
from simulate import run_simulation

app = FastAPI(title="TalentIQ", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()


@app.post("/api/ingest")
async def ingest(file: UploadFile = File(...)):
    try:
        data = await file.read()
        result = await ingest_resume(file.filename, data)
        return result
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/search")
async def search(
    q: str = Query(..., min_length=1),
    clearance: str = Query(default=""),
    min_exp: int = Query(default=0),
    max_exp: int = Query(default=99),
):
    results = await search_candidates(q, clearance=clearance, min_exp=min_exp, max_exp=max_exp)
    return results


@app.get("/api/candidates")
async def candidates():
    return await list_candidates()


@app.delete("/api/candidates")
async def clear_candidates():
    try:
        from db import get_conn
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM resume_chunks")
            cur.execute("DELETE FROM candidates")
            conn.commit()
        return {"status": "ok", "message": "All candidates removed"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/chat")
async def chat(body: dict):
    message = body.get("message", "").strip()
    if not message:
        return JSONResponse(status_code=400, content={"error": "Message required"})
    try:
        result = await chat_with_candidates(message)
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/simulate")
async def simulate(count: int = Query(default=10000, ge=1, le=50000)):
    return StreamingResponse(
        run_simulation(count),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# Serve UI static files
ui_path = os.path.join(os.path.dirname(__file__), "..", "ui")
if os.path.isdir(ui_path):
    app.mount("/", StaticFiles(directory=ui_path, html=True), name="ui")
