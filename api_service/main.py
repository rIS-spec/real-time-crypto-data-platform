# PURPOSE: Creates and starts the FastAPI application
# File: api_service/main.py


from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from api_service.routes import router
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Real-Time Crypto Data Platform",
    description="Live crypto prices — Kafka pipeline + PostgreSQL",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(router)   # it is a connection between main.py and routes.py


# step 1 — Create FastAPI app
@app.get("/")
def root():
    return {
        "message": "Crypto Data Platform is running!",
        "docs": "/docs",
        "version": "1.0.0"
    }


# step 2 — Add startup and shutdown events for logging
@app.on_event("startup")
async def startup_event():    # called when server starts up automatically by uvicorn, async = non-blocking I/O from the OS kernel to run in the background without blocking the main thread of execution 
    logger.info("Starting Crypto Data Platform...")
    logger.info("Kafka: localhost:9092")
    logger.info("PostgreSQL: connected")
    logger.info("API ready at http://localhost:8000")
    logger.info("Waiting for requests...")


# step 3 — Add shutdown event for logging
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down Crypto Data Platform...")
    logger.info("Closing all connections cleanly")
    logger.info("Goodbye!")


# step 4 — Add global exception handler for logging
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": str(exc)}
    )




