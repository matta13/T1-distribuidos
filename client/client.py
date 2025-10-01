import os
import sys
import argparse
import httpx

def main():
    parser = argparse.ArgumentParser(description="Cliente QA (imprime score y respuesta)")
    parser.add_argument("--api", default=os.getenv("API_URL", "http://qa_api:8000/ask"),
                        help="URL de la API /ask (por defecto: %(default)s)")
    parser.add_argument("--q", "--question", dest="question", default=None,
                        help="Pregunta a enviar; si no se pasa, se pedirá por teclado")
    args = parser.parse_args()

    # Soporte UTF-8 en Windows
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    question = (args.question or input("Pregunta: ")).strip()
    if not question:
        print("No se ingresó pregunta.")
        sys.exit(1)

    payload = {"question": question}

    try:
        with httpx.Client(timeout=60) as client:
            r = client.post(args.api, json=payload)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        print(f"Error llamando a la API: {e}")
        sys.exit(2)

    row = (data or {}).get("row", {})
    score = row.get("score")
    answer = row.get("answer")

    if score is None or answer is None:
        print("Respuesta inesperada de la API (faltan campos).")
        sys.exit(3)

    print(f"Score: {score}\nRespuesta: {answer}")

if __name__ == "__main__":
    main()

