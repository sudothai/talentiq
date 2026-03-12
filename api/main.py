from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

from db import init_db
from ingest import ingest_resume
from search import search_candidates, list_candidates

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
async def search(q: str = Query(..., min_length=1)):
    results = await search_candidates(q)
    return results


@app.get("/api/candidates")
async def candidates():
    return await list_candidates()


# Serve UI static files
ui_path = os.path.join(os.path.dirname(__file__), "..", "ui")
if os.path.isdir(ui_path):
    app.mount("/", StaticFiles(directory=ui_path, html=True), name="ui")
