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
- AIS_DIAGNOSTIC_GLOBAL_BBOX=false
- AIS_DIAGNOSTIC_ALL_MESSAGE_TYPES=false

## Run

uv run hormuz-tracker

The command starts FastAPI and opens the app in your default browser:

http://127.0.0.1:8000

To start the server without opening a browser:

OPEN_BROWSER=false uv run hormuz-tracker

## Regions

The app starts with the Strait of Hormuz by default. Change region from the dropdown in the frontend; the backend reconnects to aisstream.io using the selected region's bounding box, then the frontend recenters the map and redraws the geofence.

Available regions:

- Strait of Hormuz
- Sicily and Central Mediterranean
- Gulf of Oman
- London and Thames Estuary
- Gibraltar Strait
- Iceland Coastal Waters
- Black Sea
- Crimea and Kerch Strait
- Singapore Strait
- Suez Canal
- Panama Canal
- English Channel

## Global CLI Install 
uv tool install -e . 

hormuz-tracker 

Uninstall: 

uv tool uninstall hormuz-tracker
