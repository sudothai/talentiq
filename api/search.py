import json
import os

import httpx

from db import get_conn

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

RERANK_SYSTEM = (
    "You are a recruiting assistant. Given a search query and a list of candidates, "
    "return the top 5 most relevant candidates ranked by fit. For each, write one "
    "sentence explaining why they match. Return ONLY valid JSON array: "
    '[{ "candidate_id": "", "rank": 1, "explanation": "" }]'
)


def embed_text(text: str) -> list[float]:
    resp = httpx.post(
        f"{OLLAMA_BASE_URL}/api/embeddings",
        json={"model": "nomic-embed-text", "prompt": text},
        timeout=60.0,
    )
    resp.raise_for_status()
    return resp.json()["embedding"]


def strip_json(text: str) -> str:
    import re
    text = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if m:
        text = m.group(1).strip()
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        s = text.find(start_char)
        e = text.rfind(end_char)
        if s != -1 and e != -1:
            return text[s : e + 1]
    return text


async def search_candidates(query: str, clearance: str = "", min_exp: int = 0, max_exp: int = 99) -> list[dict]:
    query_embedding = embed_text(query)

    # Vector similarity search — top 20
    filters = []
    params = [str(query_embedding), str(query_embedding)]

    if clearance:
        filters.append("c.clearance = %s")
        params.append(clearance)
    if min_exp > 0:
        filters.append("c.years_experience >= %s")
        params.append(min_exp)
    if max_exp < 99:
        filters.append("c.years_experience <= %s")
        params.append(max_exp)

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT ON (c.id)
                c.id, c.name, c.skills, c.years_experience, c.titles,
                1 - (rc.embedding <=> %s::vector) AS score, c.clearance
            FROM resume_chunks rc
            JOIN candidates c ON c.id = rc.candidate_id
            {where_clause}
            ORDER BY c.id, rc.embedding <=> %s::vector
            """,
            params,
        )
        rows = cur.fetchall()

    # Sort by score descending, take top 20
    rows.sort(key=lambda r: r[5], reverse=True)
    top20 = rows[:20]

    if not top20:
        return []

    # Build context for reranking
    candidates_text = "\n".join(
        f"- candidate_id: {r[0]}, name: {r[1]}, skills: {r[2]}, "
        f"titles: {r[4]}, years_experience: {r[3]}, clearance: {r[6] or 'None'}, similarity: {r[5]:.3f}"
        for r in top20
    )

    rerank_prompt = (
        f"Search query: {query}\n\nCandidates:\n{candidates_text}\n\n"
        "Return the top 5 most relevant candidates as JSON."
    )

    resp = httpx.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={
            "model": "llama3.2:8b",
            "prompt": rerank_prompt,
            "system": RERANK_SYSTEM,
            "stream": False,
        },
        timeout=120.0,
    )
    resp.raise_for_status()
    raw = resp.json()["response"]
    reranked = json.loads(strip_json(raw))

    # Enrich with candidate metadata
    candidate_map = {str(r[0]): r for r in top20}
    results = []
    for item in reranked[:5]:
        cid = item["candidate_id"]
        row = candidate_map.get(cid)
        if row:
            results.append({
                "candidate_id": cid,
                "name": row[1],
                "score": round(row[5], 3),
                "explanation": item.get("explanation", ""),
                "skills": row[2] or [],
                "years_experience": row[3],
                "clearance": row[6] or "",
            })

    return results


async def list_candidates() -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, name, email, skills, titles, years_experience, created_at, clearance
               FROM candidates ORDER BY created_at DESC"""
        )
        rows = cur.fetchall()

    return [
        {
            "candidate_id": str(r[0]),
            "name": r[1],
            "email": r[2],
            "skills": r[3] or [],
            "titles": r[4] or [],
            "years_experience": r[5],
            "created_at": r[6].isoformat() if r[6] else None,
            "clearance": r[7] or "",
        }
        for r in rows
    ]
