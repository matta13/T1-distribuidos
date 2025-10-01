# -*- coding: utf-8 -*-
import hashlib
import json
import os
from typing import Optional, Literal

import httpx
import psycopg2
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# =========================
# Configuración
# =========================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_db")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

REDIS_HOST = os.getenv("REDIS_HOST", "redis_cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "604800"))  # 7 días

# =========================
# Conexiones globales
# =========================
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

_db_conn = None
def get_db_conn():
    global _db_conn
    if _db_conn is None or getattr(_db_conn, "closed", 1) != 0:
        _db_conn = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        _db_conn.autocommit = True
    return _db_conn

# =========================
# Modelos
# =========================
class AskRequest(BaseModel):
    question: str

class Row(BaseModel):
    score: int
    title: str
    body: Optional[str]
    answer: str

class AskResponse(BaseModel):
    source: Literal["cache", "db", "llm"]
    row: Row
    message: str

# =========================
# Utilidades
# =========================
def normalize_question(q: str) -> str:
    return " ".join(q.strip().lower().split())

def cache_key_for(q: str) -> str:
    norm = normalize_question(q)
    h = hashlib.sha256(norm.encode("utf-8")).hexdigest()
    return f"qa:{h}"

def row_to_message(row: Row) -> str:
    return "\n".join([
        f"Pregunta: {row.title}",
        f"Respuesta: {row.answer}",
        f"Score (1-10): {row.score}",
    ])

# =========================
# Redis y Postgres
# =========================
def get_from_cache(question: str) -> Optional[Row]:
    try:
        data = r.get(cache_key_for(question))
    except Exception:
        return None
    if not data:
        return None
    try:
        return Row(**json.loads(data))
    except Exception:
        return None

def set_to_cache(question: str, row: Row):
    try:
        r.setex(cache_key_for(question), CACHE_TTL_SECONDS, row.model_dump_json())
    except Exception:
        pass

def get_from_db(question: str) -> Optional[Row]:
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT score, title, body, answer
            FROM querys
            WHERE LOWER(title) = LOWER(%s)
            LIMIT 1
            """,
            (question,),
        )
        res = cur.fetchone()
        if res:
            return Row(score=res[0], title=res[1], body=res[2], answer=res[3])
        return None

def upsert_row(row: Row) -> None:
    """
    Si existe una fila con mismo title (case-insensitive) y mismo body (NULL-safe),
    hace UPDATE; si no, INSERT. (Solo 4 columnas)
    """
    conn = get_db_conn()
    with conn.cursor() as cur:
        # UPDATE primero (match por title+body NULL-safe)
        cur.execute(
            """
            UPDATE querys
               SET score = %s,
                   answer = %s,
                   body = %s
             WHERE LOWER(title) = LOWER(%s)
               AND (
                     (body IS NULL AND %s IS NULL) OR
                     (body = %s)
                   )
            """,
            (row.score, row.answer, row.body, row.title, row.body, row.body),
        )
        if cur.rowcount == 0:
            # No había fila igual -> INSERT
            cur.execute(
                """
                INSERT INTO querys (score, title, body, answer)
                VALUES (%s, %s, %s, %s)
                """,
                (row.score, row.title, row.body, row.answer),
            )

# =========================
# PROMPT (sin triple comillas para evitar indent issues)
# =========================
PROMPT_TEMPLATE = (
    "Responde la pregunta del usuario y calcula UN solo puntaje final de 1 a 10 considerando internamente:\n"
    "- Exactitud (40%)\n- Integridad (25%)\n- Claridad (20%)\n- Concisión (10%)\n- Utilidad (5%)\n\n"
    "Devuelve EXCLUSIVAMENTE un JSON válido en este FORMATO y ORDEN (sin texto extra):\n"
    "[\n  final_score_entero_1_a_10,\n  \"<repite la pregunta EXACTAMENTE como la recibiste>\",\n  null,\n  \"<respuesta en texto>\"\n]\n\n"
    "No incluyas desgloses ni comentarios.\n\n"
    "Pregunta:\n"
)

# =========================
# Llamada a Ollama
# =========================
async def ask_ollama(question: str) -> Row:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"{PROMPT_TEMPLATE}{question}",
        "stream": False,
    }
    url = f"{OLLAMA_HOST}/api/generate"
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(url, json=payload)
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Ollama error: {resp.text}")
        data = resp.json()
        raw = str(data.get("response", "")).strip()

        parsed = None
        try:
            parsed = json.loads(raw)
        except Exception:
            start = raw.find("["); end = raw.rfind("]")
            if start != -1 and end != -1 and end > start:
                try:
                    parsed = json.loads(raw[start:end+1])
                except Exception:
                    parsed = None

        if not isinstance(parsed, list) or len(parsed) != 4:
            raise HTTPException(status_code=502, detail="Formato inesperado desde Ollama: se esperaba [score, question, null, answer]")

        # 0) score entero 1..10
        try:
            final_score = int(round(float(parsed[0])))
        except Exception:
            final_score = 1
        final_score = max(1, min(10, final_score))

        # 1) pregunta (usamos la original)
        title = question

        # 3) answer string
        answer = str(parsed[3]).strip()

        return Row(score=final_score, title=title, body=None, answer=answer)

# =========================
# FastAPI
# =========================
app = FastAPI(title="QA Orchestrator API", version="1.0.4")

@app.get("/health")
def health():
    try:
        r.ping()
        get_db_conn().cursor().execute("SELECT 1")
        ok = True
    except Exception:
        ok = False
    return {"ok": ok}

@app.post("/ask", response_model=AskResponse)
async def ask(req: AskRequest):
    q = req.question.strip()
    if not q:
        raise HTTPException(status_code=400, detail="Pregunta vacía")

    # 1) Cache
    row = get_from_cache(q)
    if row:
        return AskResponse(source="cache", row=row, message=row_to_message(row))

    # 2) BD
    row = get_from_db(q)
    if row:
        set_to_cache(q, row)
        return AskResponse(source="db", row=row, message=row_to_message(row))

    # 3) LLM
    row = await ask_ollama(q)
    upsert_row(row)
    set_to_cache(q, row)
    return AskResponse(source="llm", row=row, message=row_to_message(row))
