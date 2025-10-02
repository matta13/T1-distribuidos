import hashlib
import json
import os
from typing import Optional, Literal

import httpx
import psycopg2
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Configuración
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_db")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

REDIS_HOST = os.getenv("REDIS_HOST", "redis_cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "20"))  

# Conexiones globales
redis_cliente = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

_conexion_db = None
def obtener_conexion_db():
    """Obtiene (y mantiene) una conexión global a Postgres."""
    global _conexion_db
    if _conexion_db is None or getattr(_conexion_db, "closed", 1) != 0:
        _conexion_db = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        _conexion_db.autocommit = True
    return _conexion_db

# Modelos
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

# Utilidades
def normalizar_pregunta(pregunta: str) -> str:
    return " ".join(pregunta.strip().lower().split())

def clave_cache_para(pregunta: str) -> str:
    normalizada = normalizar_pregunta(pregunta)
    hash_hex = hashlib.sha256(normalizada.encode("utf-8")).hexdigest()
    return f"qa:{hash_hex}"

def fila_a_mensaje(fila: Row) -> str:
    return "\n".join([
        f"Pregunta: {fila.title}",
        f"Respuesta: {fila.answer}",
        f"Score (1-10): {fila.score}",
    ])

# Redis y Postgres
def leer_desde_cache(pregunta: str) -> Optional[Row]:
    try:
        datos = redis_cliente.get(clave_cache_para(pregunta))
    except Exception:
        return None
    if not datos:
        return None
    try:
        return Row(**json.loads(datos))
    except Exception:
        return None

def escribir_en_cache(pregunta: str, fila: Row):
    try:
        redis_cliente.setex(clave_cache_para(pregunta), CACHE_TTL_SECONDS, fila.model_dump_json())
    except Exception:
        pass

def leer_desde_db(pregunta: str) -> Optional[Row]:
    conexion = obtener_conexion_db()
    with conexion.cursor() as cursor:
        cursor.execute(
            """
            SELECT score, title, body, answer
            FROM querys
            WHERE LOWER(title) = LOWER(%s)
            LIMIT 1
            """,
            (pregunta,),
        )
        resultado = cursor.fetchone()
        if resultado:
            return Row(score=resultado[0], title=resultado[1], body=resultado[2], answer=resultado[3])
        return None

def upsert_fila(fila: Row) -> None:
    """
    Si existe una fila con mismo title (case-insensitive) y mismo body (NULL-safe),
    hace UPDATE; si no, INSERT. (Solo 4 columnas)
    """
    conexion = obtener_conexion_db()
    with conexion.cursor() as cursor:
        cursor.execute(
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
            (fila.score, fila.answer, fila.body, fila.title, fila.body, fila.body),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO querys (score, title, body, answer)
                VALUES (%s, %s, %s, %s)
                """,
                (fila.score, fila.title, fila.body, fila.answer),
            )

# PROMPT 
PLANTILLA_PROMPT = (
    "Responde la pregunta del usuario y calcula UN solo puntaje final de 1 a 10 considerando internamente:\n"
    "- Exactitud (40%)\n- Integridad (25%)\n- Claridad (20%)\n- Concisión (10%)\n- Utilidad (5%)\n\n"
    "Devuelve EXCLUSIVAMENTE un JSON válido en este FORMATO y ORDEN (sin texto extra):\n"
    "[\n  final_score_entero_1_a_10,\n  \"<repite la pregunta EXACTAMENTE como la recibiste>\",\n  null,\n  \"<respuesta en texto>\"\n]\n\n"
    "No incluyas desgloses ni comentarios.\n\n"
    "Pregunta:\n"
)

# Llamada a Ollama
async def consultar_ollama(pregunta: str) -> Row:
    cuerpo_solicitud = {
        "model": OLLAMA_MODEL,
        "prompt": f"{PLANTILLA_PROMPT}{pregunta}",
        "stream": False,
    }
    url_ollama = f"{OLLAMA_HOST}/api/generate"
    async with httpx.AsyncClient(timeout=120) as cliente_http:
        respuesta = await cliente_http.post(url_ollama, json=cuerpo_solicitud)
        if respuesta.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Ollama error: {respuesta.text}")
        datos = respuesta.json()
        bruto = str(datos.get("response", "")).strip()

        parsed = None
        try:
            parsed = json.loads(bruto)
        except Exception:
            inicio = bruto.find("["); fin = bruto.rfind("]")
            if inicio != -1 and fin != -1 and fin > inicio:
                try:
                    parsed = json.loads(bruto[inicio:fin+1])
                except Exception:
                    parsed = None

        if not isinstance(parsed, list) or len(parsed) != 4:
            raise HTTPException(status_code=502, detail="Formato inesperado desde Ollama: se esperaba [score, question, null, answer]")
            
        try:
            puntaje_final = int(round(float(parsed[0])))
        except Exception:
            puntaje_final = 1
        puntaje_final = max(1, min(10, puntaje_final))
        
        titulo = pregunta
        respuesta_texto = str(parsed[3]).strip()

        return Row(score=puntaje_final, title=titulo, body=None, answer=respuesta_texto)

# FastAPI
app = FastAPI(title="QA Orchestrator API", version="1.0.4")

@app.get("/health")
def health():
    try:
        redis_cliente.ping()
        obtener_conexion_db().cursor().execute("SELECT 1")
        estado_ok = True
    except Exception:
        estado_ok = False
    return {"ok": estado_ok}

@app.post("/ask", response_model=AskResponse)
async def ask(solicitud: AskRequest):
    pregunta = solicitud.question.strip()
    if not pregunta:
        raise HTTPException(status_code=400, detail="Pregunta vacía")

    fila = leer_desde_cache(pregunta)
    if fila:
        return AskResponse(source="cache", row=fila, message=fila_a_mensaje(fila))

    fila = leer_desde_db(pregunta)
    if fila:
        escribir_en_cache(pregunta, fila)
        return AskResponse(source="db", row=fila, message=fila_a_mensaje(fila))
        
    fila = await consultar_ollama(pregunta)
    upsert_fila(fila)
    escribir_en_cache(pregunta, fila)
    return AskResponse(source="llm", row=fila, message=fila_a_mensaje(fila))


