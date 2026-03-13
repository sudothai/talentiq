import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

POSTGRES_URL = os.environ.get(
    "POSTGRES_URL",
    "postgresql://talentiq:talentiq123@localhost:5432/talentiq",
)

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")

_pool = None
_qdrant = None


def get_pool():
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(1, 10, POSTGRES_URL)
    return _pool


@contextmanager
def get_conn():
    p = get_pool()
    conn = p.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def get_qdrant() -> QdrantClient:
    global _qdrant
    if _qdrant is None:
        _qdrant = QdrantClient(url=QDRANT_URL, timeout=60)
    return _qdrant


def init_db():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name TEXT,
                email TEXT,
                skills TEXT[],
                titles TEXT[],
                years_experience INT,
                clearance TEXT,
                education JSONB,
                raw_file_path TEXT,
                processed_file_path TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        # Migration: add clearance column if missing
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'candidates' AND column_name = 'clearance'
                ) THEN
                    ALTER TABLE candidates ADD COLUMN clearance TEXT;
                END IF;
            END $$;
        """)
        conn.commit()


def init_qdrant():
    client = get_qdrant()
    collections = [c.name for c in client.get_collections().collections]
    if "resume_chunks" not in collections:
        client.create_collection(
            collection_name="resume_chunks",
            vectors_config=VectorParams(
                size=768,
                distance=Distance.COSINE,
            ),
        )
