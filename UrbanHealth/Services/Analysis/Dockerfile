FROM python:3.11-slim
WORKDIR /app
# FORÇA O PYTHON A MOSTRAR OS LOGS IMEDIATAMENTE NO DOCKER
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# O ficheiro server.py já tem de ter os _pb2 gerados
CMD ["python", "server.py"]