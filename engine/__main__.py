"""python -m engine — resident trading engine."""

import uvicorn

from engine.app import create_app
from engine.config import EngineConfig


def main():
    config = EngineConfig.from_environment()
    uvicorn.run(create_app(config), host=config.host, port=config.port)


if __name__ == "__main__":
    main()
