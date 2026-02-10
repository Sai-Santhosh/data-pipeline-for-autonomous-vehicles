#!/usr/bin/env bash
# Fleet Data Pipeline — Quickstart (Linux/macOS)
# Run from repo root: ./scripts/quickstart.sh
# 1) Starts Docker  2) Creates Kafka topics  3) Runs verify  4) Prints next steps

set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -f "docker-compose.yml" ]; then
  echo "Run from repo root. docker-compose.yml not found."
  exit 1
fi

echo "=== Fleet Data Pipeline — Quickstart ==="
echo ""

echo "1. Starting Docker (Kafka + TimescaleDB)..."
docker-compose up -d
echo "   Waiting 15s for services..."
sleep 15

echo ""
echo "2. Creating Kafka topics..."
chmod +x scripts/create_topics.sh 2>/dev/null || true
./scripts/create_topics.sh || { echo "   Retrying in 5s..."; sleep 5; ./scripts/create_topics.sh; }

echo ""
echo "3. Running verify.py..."
python scripts/verify.py
VERIFY_EXIT=$?

echo ""
echo "=== Next steps ==="
echo "Open 3 terminals and run (from repo root):"
echo "  Terminal 1: python scripts/run_consumer.py"
echo "  Terminal 2: python scripts/run_producer.py live"
echo "  Terminal 3: python scripts/run_dashboard.py"
echo "Then open http://localhost:8501"
echo ""
exit $VERIFY_EXIT
