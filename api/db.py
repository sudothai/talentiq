import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager

POSTGRES_URL = os.environ.get(
    "POSTGRES_URL",
    "postgresql://talentiq:talentiq123@localhost:5432/talentiq",
)

_pool = None


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


def init_db():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS resume_chunks (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                candidate_id UUID REFERENCES candidates(id),
                chunk_text TEXT,
                embedding vector(768),
                section TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_resume_chunks_embedding
            ON resume_chunks USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 10);
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
