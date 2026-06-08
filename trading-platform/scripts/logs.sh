#!/bin/bash
# Follow logs for a specific service or all services
cd "$(dirname "$0")/.."

if [ -z "$1" ]; then
  echo "Following all logs (Ctrl+C to stop)..."
  docker compose logs -f --tail=100
else
  echo "Following logs for: $1"
  docker compose logs -f --tail=100 "$1"
fi
