"""
Main FastAPI application entrypoint.

This module initializes the FastAPI app, loads configuration,
sets up middleware, and registers all API routers.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from app.config import get_settings
from app.router import router


def create_app() -> FastAPI:
    """
    Application factory.

    Creates and configures the FastAPI application using
    centralized Settings.
    """
    settings = get_settings()

    # Ensure required directories exist on startup
    settings.ensure_directories()

    app = FastAPI(
        title=settings.api_title,
        version=settings.api_version,
    )

    # CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register routers
    app.include_router(
        router,
        prefix=settings.api_prefix,
        tags=["HDX SSD Pipeline"],
    )

    return app


# FastAPI application instance
app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "main_fastapi:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
    )
