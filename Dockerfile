# --- Build Stage ---
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --user --no-cache-dir -r requirements.txt

COPY . .

# --- Runtime Stage ---
FROM python:3.11-slim AS runtime

WORKDIR /app

# install runtime libpq for psycopg2 to work
RUN apt-get update && apt-get install -y libpq5 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.local /root/.local
COPY --from=builder /app /app

# 支持 build-arg 动态注入 .env
ARG ENV_FILE=.env.sv
COPY ${ENV_FILE} .env
ENV ENV_FILE=.env

ENV PATH=/root/.local/bin:$PATH

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]