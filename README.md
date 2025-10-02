-En primer lugar, se debe clonar el repositorio.

-En segundo lugar, se accede desde cmd al directorio donde se encuentra la carpeta.

-paso 3, levantar contenedores con el comando: docker compose up -d.

-paso 4, hacer un pull del modelo llama3 con el comando: docker exec -it ollama ollama pull llama3.

-paso 5, escribir el siguiente comando para ingresar al contenedor del cliente: docker exec -it qa_client sh.

-paso 6, escribir el comando: python3 client.py para ejecutar el codigo del cliente y aparecera un espacio para escribir preguntas.

-La aplicacion ya esta lista para realizar consultas tanto de las que se encuentran en la base como otras preguntas.

***tener cuidado al clonar los repositorios ya que a veces .env.api no se copia correctamente***
