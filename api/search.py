import json
import os
import traceback

import httpx

from db import get_conn

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

RERANK_SYSTEM = (
    "You are a recruiting assistant. Given a search query and a list of candidates, "
    "return the top 5 most relevant candidates ranked by fit. For each, write one "
    "sentence explaining why they match. Return ONLY valid JSON array: "
    '[{ "candidate_id": "", "rank": 1, "explanation": "" }]'
)

CHAT_SYSTEM = (
    "You are TalentIQ, an AI recruiting assistant. You have access to a database of candidates. "
    "The user will ask questions about hiring, candidates, or talent search. "
    "You will be given relevant candidate data from vector search results. "
    "Use this data to answer the user's question conversationally. "
    "Be specific — mention candidate names, skills, clearances, and experience levels. "
    "If the data doesn't fully answer the question, say so honestly."
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


def _vector_search(query_embedding, clearance="", min_exp=0, max_exp=99, limit=20):
    """Run vector similarity search with optional filters."""
    emb = str(query_embedding)
    filters = []
    filter_params = []

    if clearance:
        filters.append("c.clearance = %s")
        filter_params.append(clearance)
    if min_exp > 0:
        filters.append("c.years_experience >= %s")
        filter_params.append(min_exp)
    if max_exp < 99:
        filters.append("c.years_experience <= %s")
        filter_params.append(max_exp)

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    # Parameter order must match SQL placeholder order:
    # 1st %s = SELECT embedding, 2nd+ = WHERE filters, last %s = ORDER BY embedding
    params = [emb] + filter_params + [emb]

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

    rows.sort(key=lambda r: r[5], reverse=True)
    return rows[:limit]


def _row_to_result(row, explanation=""):
    return {
        "candidate_id": str(row[0]),
        "name": row[1],
        "score": round(row[5], 3),
        "explanation": explanation,
        "skills": row[2] or [],
        "years_experience": row[3],
        "clearance": row[6] or "",
    }


def _llm_rerank(query, top20):
    """Ask LLM to rerank candidates. Returns reranked list or None on failure."""
    candidates_text = "\n".join(
        f"- candidate_id: {r[0]}, name: {r[1]}, skills: {r[2]}, "
        f"titles: {r[4]}, years_experience: {r[3]}, clearance: {r[6] or 'None'}, similarity: {r[5]:.3f}"
        for r in top20
    )

    rerank_prompt = (
        f"Search query: {query}\n\nCandidates:\n{candidates_text}\n\n"
        "Return the top 5 most relevant candidates as JSON."
    )

    try:
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": "llama3.1:8b",
                "prompt": rerank_prompt,
                "system": RERANK_SYSTEM,
                "stream": False,
            },
            timeout=120.0,
        )
        resp.raise_for_status()
        raw = resp.json()["response"]
        return json.loads(strip_json(raw))
    except Exception:
        traceback.print_exc()
        return None


async def search_candidates(query: str, clearance: str = "", min_exp: int = 0, max_exp: int = 99) -> list[dict]:
    query_embedding = embed_text(query)
    top20 = _vector_search(query_embedding, clearance, min_exp, max_exp)

    if not top20:
        return []

    # Try LLM rerank
    reranked = _llm_rerank(query, top20)

    if reranked:
        candidate_map = {str(r[0]): r for r in top20}
        results = []
        for item in reranked[:5]:
            cid = item.get("candidate_id", "")
            row = candidate_map.get(cid)
            if row:
                results.append(_row_to_result(row, item.get("explanation", "")))
        if results:
            return results

    # Fallback: return top 5 by vector similarity
    return [_row_to_result(r, "Matched by vector similarity") for r in top20[:5]]


async def chat_with_candidates(message: str) -> dict:
    """RAG chat: embed query, retrieve candidates, generate conversational response."""
    query_embedding = embed_text(message)
    top_candidates = _vector_search(query_embedding, limit=10)

    if not top_candidates:
        return {"response": "I don't have any candidates in the database yet. Try uploading some resumes or running the simulation first.", "candidates": []}

    # Build context for LLM
    context_lines = []
    for r in top_candidates:
        context_lines.append(
            f"- {r[1]}: skills={r[2]}, titles={r[4]}, "
            f"years_exp={r[3]}, clearance={r[6] or 'None'}, match_score={r[5]:.3f}"
        )
    context = "\n".join(context_lines)

    prompt = (
        f"Here are the most relevant candidates from the database:\n\n{context}\n\n"
        f"User question: {message}\n\n"
        "Answer the question using the candidate data above. Be helpful and specific."
    )

    try:
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": "llama3.1:8b",
                "prompt": prompt,
                "system": CHAT_SYSTEM,
                "stream": False,
            },
            timeout=120.0,
        )
        resp.raise_for_status()
        answer = resp.json()["response"]
    except Exception:
        traceback.print_exc()
        answer = "I found some matching candidates but had trouble generating a detailed response. Here are the top matches:"

    candidates = [_row_to_result(r) for r in top_candidates[:5]]
    return {"response": answer, "candidates": candidates}


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
