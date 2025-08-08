import os
import random

import httpx
from fastapi import FastAPI, Request, Response
from starlette.responses import Response as StarletteResponse, JSONResponse

app = FastAPI(title="CinemaAbyss Proxy Service", version="1.0.0")

PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))  # 0-100

client = httpx.AsyncClient(timeout=30.0)

@app.on_event("shutdown")
async def _shutdown():
    await client.aclose()

@app.get("/health", tags=["health"])
async def health_check():
    return Response(content="Strangler Fig Proxy is healthy", media_type="text/plain")

def _select_movies_backend() -> str:
    if not GRADUAL_MIGRATION:
        return MONOLITH_URL
    roll = random.randint(1, 100)
    if roll <= MOVIES_MIGRATION_PERCENT:
        return MOVIES_SERVICE_URL
    return MONOLITH_URL

async def _forward(request: Request, base_url: str) -> StarletteResponse:
    url = httpx.URL(base_url + request.url.path)
    if request.url.query:
        url = url.copy_with(query=request.url.query)

    headers = {key: value for key, value in request.headers.items() if key.lower() not in {"host", "content-length"}}

    try:
        resp = await client.request(
            request.method,
            url,
            content=await request.body(),
            headers=headers,
        )
        return StarletteResponse(content=resp.content, status_code=resp.status_code, headers=resp.headers)
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502, content={"error": f"Upstream request failed: {exc}"})


@app.api_route("/api/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def proxy_api(full_path: str, request: Request):
    original_path = "/api/" + full_path

    if original_path.startswith("/api/movies"):
        backend = _select_movies_backend()
    elif original_path.startswith("/api/events"):
        backend = EVENTS_SERVICE_URL
    else:
        backend = MONOLITH_URL

    return await _forward(request, backend)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False) 