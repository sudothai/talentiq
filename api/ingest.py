import io
import json
import os
import re
import uuid

import httpx
import pdfplumber
from docx import Document
from minio import Minio

from db import get_conn

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "talentiq")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "talentiq123")
BUCKET = "resumes"

EXTRACT_SYSTEM = (
    "You are a resume parser. Extract structured data from the resume text "
    "and return ONLY valid JSON with no other text.\n"
    'Schema: { "name": "", "email": "", "skills": [], "titles": [], '
    '"years_experience": 0, "education": [], '
    '"clearance": "" }\n'
    'For clearance, extract any security clearance level mentioned '
    '(e.g. "Top Secret/SCI", "Top Secret", "Secret", "Confidential", "Public Trust"). '
    'Leave empty string if none mentioned.'
)


def get_minio():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def ensure_bucket(client):
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)


def parse_pdf(data: bytes) -> str:
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def parse_docx(data: bytes) -> str:
    doc = Document(io.BytesIO(data))
    return "\n".join(p.text for p in doc.paragraphs)


def strip_json(text: str) -> str:
    """Strip markdown fences and prose around JSON."""
    text = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if m:
        text = m.group(1).strip()
    for start_char, end_char in [("{", "}"), ("[", "]")]:
        s = text.find(start_char)
        e = text.rfind(end_char)
        if s != -1 and e != -1:
            return text[s : e + 1]
    return text


def call_ollama(prompt: str, system: str, model: str = "llama3.1:8b") -> str:
    resp = httpx.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={"model": model, "prompt": prompt, "system": system, "stream": False},
        timeout=120.0,
    )
    resp.raise_for_status()
    return resp.json()["response"]


def embed_text(text: str) -> list[float]:
    resp = httpx.post(
        f"{OLLAMA_BASE_URL}/api/embeddings",
        json={"model": "nomic-embed-text", "prompt": text},
        timeout=60.0,
    )
    resp.raise_for_status()
    return resp.json()["embedding"]


def chunk_resume(text: str) -> list[dict]:
    """Split resume into section-based chunks."""
    section_headers = [
        "summary", "objective", "experience", "work experience",
        "education", "skills", "projects", "certifications",
        "awards", "publications", "references", "contact",
    ]
    lines = text.split("\n")
    chunks = []
    current_section = "header"
    current_lines = []

    for line in lines:
        stripped = line.strip().lower().rstrip(":")
        if stripped in section_headers:
            if current_lines:
                chunk_text = "\n".join(current_lines).strip()
                if chunk_text:
                    chunks.append({"section": current_section, "text": chunk_text})
            current_section = stripped
            current_lines = [line]
        else:
            current_lines.append(line)

    if current_lines:
        chunk_text = "\n".join(current_lines).strip()
        if chunk_text:
            chunks.append({"section": current_section, "text": chunk_text})

    if not chunks:
        chunks.append({"section": "full", "text": text.strip()})

    return chunks


async def ingest_resume(filename: str, data: bytes) -> dict:
    candidate_id = str(uuid.uuid4())
    ext = filename.rsplit(".", 1)[-1].lower()

    if ext == "pdf":
        text = parse_pdf(data)
    elif ext in ("docx", "doc"):
        text = parse_docx(data)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    if not text.strip():
        raise ValueError("Could not extract text from file")

    # Upload raw file to MinIO
    mc = get_minio()
    ensure_bucket(mc)
    raw_path = f"raw/{candidate_id}/{filename}"
    mc.put_object(BUCKET, raw_path, io.BytesIO(data), len(data))

    # Extract structured data via Ollama
    raw_response = call_ollama(text[:4000], EXTRACT_SYSTEM)
    extracted = json.loads(strip_json(raw_response))

    # Save processed JSON to MinIO
    processed_path = f"processed/{candidate_id}/extracted.json"
    processed_bytes = json.dumps(extracted, indent=2).encode()
    mc.put_object(BUCKET, processed_path, io.BytesIO(processed_bytes), len(processed_bytes))

    # Insert candidate into DB
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO candidates
               (id, name, email, skills, titles, years_experience, clearance,
                education, raw_file_path, processed_file_path)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                candidate_id,
                extracted.get("name", ""),
                extracted.get("email", ""),
                extracted.get("skills", []),
                extracted.get("titles", []),
                extracted.get("years_experience", 0),
                extracted.get("clearance", ""),
                json.dumps(extracted.get("education", [])),
                raw_path,
                processed_path,
            ),
        )

        # Chunk, embed, and store
        chunks = chunk_resume(text)
        for chunk in chunks:
            embedding = embed_text(chunk["text"][:2000])
            cur.execute(
                """INSERT INTO resume_chunks
                   (candidate_id, chunk_text, embedding, section)
                   VALUES (%s, %s, %s::vector, %s)""",
                (candidate_id, chunk["text"], str(embedding), chunk["section"]),
            )

        conn.commit()

    return {
        "candidate_id": candidate_id,
        "name": extracted.get("name", ""),
        "chunks_stored": len(chunks),
    }
