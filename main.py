"""
Saudi Property Aggregator — FastAPI backend
Entry point: creates app, wires CORS, includes property and broker routers.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from property_scraper import router as property_router
from broker_scraper import router as broker_router

app = FastAPI(title="Saudi Property Aggregator", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(property_router)
app.include_router(broker_router)
