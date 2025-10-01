FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir httpx
COPY client.py /app/client.py
ENV API_URL=http://qa_api:8000/ask
CMD ["python", "/app/client.py"]
