FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir openpyxl

COPY process_bank_excel.py .
COPY api_server.py .

RUN mkdir /app/state

EXPOSE 5001

CMD ["python", "-u", "api_server.py"]