import os
import sys
import argparse
import httpx

API = "http://qa_api:8000/ask"

def main():
    parser = argparse.ArgumentParser(description="Cliente de Preguntas y Respuestas (imprime puntaje y respuesta)")
    parser.add_argument("--api", default=os.getenv("API_URL", "http://qa_api:8000/ask"),
                        help="URL del endpoint /ask (por defecto: %(default)s)")
    parser.add_argument("--q", "--question", dest="pregunta", default=None,
                        help="Pregunta a enviar; si no se pasa, se pedirá por teclado")
    args = parser.parse_args()

    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    texto_pregunta = (args.pregunta or input("Pregunta: ")).strip()
    if not texto_pregunta:
        print("No se ingresó una pregunta.")
        sys.exit(1)

    datos_envio = {"question": texto_pregunta}

    try:
        with httpx.Client(timeout=60) as cliente_http:
            respuesta_http = cliente_http.post(args.api, json=datos_envio)
            respuesta_http.raise_for_status()
            datos_api = respuesta_http.json()
    except Exception as error:
        print(f"Error al llamar a la API: {error}")
        sys.exit(2)

    filaR = (datos_api or {}).get("row", {})
    puntaje = filaR.get("score")
    textoR = filaR.get("answer")

    if puntaje is None or textoR is None:
        print("Respuesta inesperada de la API (faltan campos).")
        sys.exit(3)

    print(f"Puntaje: {puntaje}\nRespuesta: {textoR}")

if __name__ == "__main__":
    main()


