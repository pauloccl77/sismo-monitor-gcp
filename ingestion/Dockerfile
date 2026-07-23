FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY poller.py .

ENV POLL_INTERVAL_SECONDS=60
ENV STATE_FILE=/tmp/seen_ids.json

RUN useradd --create-home --uid 1000 poller
USER poller

CMD ["python", "poller.py"]
