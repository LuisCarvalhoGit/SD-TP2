FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Ajustar os caminhos porque o contexto agora é a pasta /Services
COPY Analysis/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código específico do serviço
COPY Analysis/ .

# Copiar a pasta shared para dentro do contentor
COPY shared/ ./shared/

CMD ["python", "server.py"]