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

app.include_router(router)

@app.get("/")
def root():
    return {
        "message": "Crypto Data Platform is running!",
        "docs": "/docs",
        "version": "1.0.0"
    }


@app.on_event("startup")
async def startup_event():
    logger.info("Starting Crypto Data Platform...")
    logger.info("Kafka: localhost:9092")
    logger.info("PostgreSQL: connected")
    logger.info("API ready at http://localhost:8000")
    logger.info("Waiting for requests...")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down Crypto Data Platform...")
    logger.info("Closing all connections cleanly")
    logger.info("Goodbye!")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": str(exc)}
    )






# File runs
#    ↓
# FastAPI app created with title, description, version
#    ↓
# CORSMiddleware added → allows frontend to call API
#    ↓
# Router connected → /crypto/health, /crypto/prices etc. active
#    ↓
# uvicorn starts server
#    ↓
# startup_event() runs automatically
#    ↓
# Server listening at localhost:8000
#    ↓
# Request comes in → routed to correct endpoint
#    ↓
# If unhandled error → global_exception_handler catches it
#    ↓
# Server stops → shutdown_event() runs automatically
#    ↓
# Connections closed cleanly
#    ↓
# Done!

# User visits /crypto/prices
#     ↓
# get_live_prices() runs
#     ↓
# Unexpected error occurs (example: memory error)
#     ↓
# FastAPI looks for exception handler
#     ↓
# global_exception_handler() catches it
#     ↓
# logger.error("Unhandled error: ...")
#     ↓
# Returns JSON:
# {
#   "success": false,
#   "error": "out of memory"
# }
# with status code 500
#     ↓
# User sees clean error, not ugly crash





#             Line by line:
# LINE: from fastapi import FastAPI, Request
# MEANS: FastAPI = main class to create the web server
#        Request = represents the incoming HTTP request
#        We need Request in the exception handler to know
#        which URL caused the error

# LINE: from fastapi.middleware.cors import CORSMiddleware
# MEANS: Middleware = code that runs on EVERY request
#        before it reaches your endpoint
#        CORSMiddleware specifically handles browser
#        security rules about who can call your API

# LINE: from fastapi.responses import JSONResponse
# MEANS: A class that creates a proper JSON HTTP response
#        We use this in the exception handler to return
#        clean JSON error messages instead of ugly crashes

# LINE: from api_service.routes import router
# MEANS: Brings in all endpoints from routes.py
#        Without this line, none of your /crypto/
#        endpoints would exist in the app

# LINE: app = FastAPI(title=..., description=..., version=...)
# MEANS: Creates the FastAPI application object
#        title = shown in /docs page
#        description = shown in /docs page
#        version = shown in /docs page
#        docs_url="/docs" = where auto documentation lives
#        redoc_url="/redoc" = alternative docs page

# LINE: app.add_middleware(CORSMiddleware, allow_origins=["*"])
# MEANS: Adds CORS rules to the entire app
#        allow_origins=["*"] = any website can call this API
#        allow_methods=["*"] = GET, POST, PUT, DELETE all allowed
#        allow_headers=["*"] = any HTTP headers allowed
#        In production change ["*"] to ["http://localhost:8501"]
#        to only allow your Streamlit dashboard

# LINE: app.include_router(router)
# MEANS: Connects routes.py to the main app
#        Like plugging a power strip into a wall socket
#        All endpoints in routes.py become available

# LINE: @app.get("/")
# MEANS: When someone visits http://localhost:8000
#        run the root() function below
#        Returns a welcome message with docs link

# LINE: @app.on_event("startup")
# MEANS: Decorator that tells FastAPI:
#        run this function BEFORE accepting any requests
#        Like a restaurant opening checklist
#        Turn on lights, check kitchen, prepare everything

# LINE: async def startup_event():
# MEANS: async = non-blocking function
#        FastAPI can handle other things while this runs
#        Logs startup messages so you know server is ready

# LINE: @app.on_event("shutdown")
# MEANS: Run this function AFTER server stops
#        Like a restaurant closing checklist
#        Close DB connections, clean up resources
#        Without this → orphaned connections in PostgreSQL

# LINE: @app.exception_handler(Exception)
# MEANS: Catches ANY unhandled error in the entire API
#        If any endpoint crashes unexpectedly
#        this function runs instead of showing ugly traceback
#        Safety net for the whole application

# LINE: async def global_exception_handler(request, exc):
# MEANS: request = the HTTP request that caused the error
#        exc = the actual exception/error that happened
#        We log the error and return clean JSON response

# LINE: return JSONResponse(status_code=500, content={...})
# MEANS: Returns a proper HTTP response with status 500
#        content = the JSON body sent back to the caller
#        {"success": False, "error": "what went wrong"}
#        Much better than an ugly Python traceback