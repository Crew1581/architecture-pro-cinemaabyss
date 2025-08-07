import os
import uuid
from datetime import datetime
import json
import asyncio
from typing import Any, Dict

from fastapi import FastAPI, Request, Response, status
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

app = FastAPI(title="CinemaAbyss Events Service", version="1.0.0")

PORT = int(os.getenv("PORT", "8082"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)

async def consume_topic(topic: str):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=f"events-service-{topic}-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    for message in consumer:
        app.logger.info(f"Consumed from {topic}: {message.value}")

@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    for topic in ["movie-events", "user-events", "payment-events"]:
        loop.create_task(asyncio.to_thread(lambda t=topic: asyncio.run(consume_topic(t))))

@app.on_event("shutdown")
async def shutdown_event():
    producer.close()

@app.get("/api/events/health", tags=["health"])
async def health():
    return {"status": True}

def _send_event(topic: str, payload: Dict[str, Any]):
    event = {
        "id": str(uuid.uuid4()),
        "type": topic.split("-")[0].replace("-events", ""),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": payload,
    }
    try:
        record_metadata = producer.send(topic, event).get(timeout=10)
        return {
            "status": "success",
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "event": event,
        }
    except KafkaError as exc:
        return {"status": "error", "error": str(exc)}

@app.post("/api/events/movie", status_code=status.HTTP_201_CREATED)
async def create_movie_event(request: Request):
    body = await request.json()
    return _send_event("movie-events", body)

@app.post("/api/events/user", status_code=status.HTTP_201_CREATED)
async def create_user_event(request: Request):
    body = await request.json()
    return _send_event("user-events", body)

@app.post("/api/events/payment", status_code=status.HTTP_201_CREATED)
async def create_payment_event(request: Request):
    body = await request.json()
    return _send_event("payment-events", body)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False) 