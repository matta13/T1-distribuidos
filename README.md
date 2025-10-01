-En primer lugar, se debe clonar el repositorio.

-En segundo lugar, se accede desde cmd al directorio donde se encuentra la carpeta.

-paso 3, levantar contenedores con el comando: docker compose up -d.

-paso 4, hacer un pull del modelo llama3 con el comando: docker exec -it ollama ollama pull llama3

-La aplicacion ya esta lista para realizar consultas tanto de las que se encuentran en la base como otras preguntas, las cuales se pueden realizar con el siguiente comando remplazando pregunta por la pregunta a realizar: curl -X POST http://localhost:8000/ask -H "Content-Type: application/json" -d "{\"question\":\"pregunta\"}" 

***tener cuidado al clonar los repositorios ya que a veces .env.api no se copia correctamente***
