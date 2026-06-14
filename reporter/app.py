import threading
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse

from aggregation import aggregate
from config import settings
from consumer import consume_loop
from schemas import PeaksResponse
from store import PeakStore

log = structlog.get_logger()

_INDEX_HTML = Path(__file__).parent / "static" / "index.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = PeakStore(settings.max_events)
    stop = threading.Event()
    worker = threading.Thread(target=consume_loop, args=(store, stop))
    worker.start()
    app.state.store = store
    log.info("reporter_started", port=settings.port)
    try:
        yield
    finally:
        stop.set()
        worker.join(timeout=5)
        log.info("reporter_stopped")


def get_store(request: Request) -> PeakStore:
    return request.app.state.store


def create_app() -> FastAPI:
    app = FastAPI(
        title="IoT Anomaly Reporter",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.get("/api/peaks", response_model=PeaksResponse)
    def api_peaks(store: PeakStore = Depends(get_store)) -> PeaksResponse:
        return aggregate(store.snapshot())

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(_INDEX_HTML)

    return app