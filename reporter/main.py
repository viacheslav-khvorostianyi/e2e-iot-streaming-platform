import uvicorn

from app import create_app
from config import settings

app = create_app()


def run() -> None:
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="warning")


if __name__ == "__main__":
    run()