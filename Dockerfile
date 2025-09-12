FROM python:3.11-slim

ARG APP=bot
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# системные зависимости по минимуму
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# код и конфиги
COPY config.ini ./               
# дефолтный конфиг (перекрывается volume-ом)
COPY main.py ./                  
# точка входа бота
COPY idk_webhook.py ./           
# сервис вебхуков от IntraDesk (uvicorn)

# команда запуска выбирается build-аргументом APP
# bot → PTB сам поднимет HTTP-сервер через run_webhook
# idk → uvicorn для FastAPI входа вебхуков Intradesk
CMD ["/bin/sh","-c","if [ \"$APP\" = \"bot\" ]; then \
      python -u main.py; \
    else \
      uvicorn idk_webhook:app --host 0.0.0.0 --port 8081 --proxy-headers; \
    fi"]
