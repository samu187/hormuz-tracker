# Hormuz Tracker

Real-time AIS ship-tracking web app for monitoring maritime traffic around the Strait of Hormuz and nearby test regions.

## Tech Stack

- Python 3.12+
- FastAPI
- WebSockets
- uv
- Leaflet.js
- aisstream.io

## Setup

- uv sync
- Create .env:
- AIS_API_KEY=your_aisstream_api_key
- AIS_REGION=hormuz
- AIS_DIAGNOSTIC_GLOBAL_BBOX=false
- AIS_DIAGNOSTIC_ALL_MESSAGE_TYPES=false

## Run
Development mode: 
uv run hormuz-tracker-dev 
Production-style local mode: 
uv run hormuz-tracker

Open: 
http://127.0.0.1:8000

Regions 
Set AIS_REGION in .env: 
AIS_REGION=hormuz

Available test regions include: 
AIS_REGION=gulf_oman_hormuz 
AIS_REGION=sicily

## Global CLI Install 
uv tool install -e . 
hormuz-tracker 
Uninstall: 
uv tool uninstall hormuz-tracker