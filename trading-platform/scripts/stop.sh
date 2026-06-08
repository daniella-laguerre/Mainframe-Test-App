#!/bin/bash
echo "Stopping all trading platform services..."
cd "$(dirname "$0")/.."
docker compose down
echo "All services stopped."
