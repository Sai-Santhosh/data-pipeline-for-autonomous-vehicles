#!/usr/bin/env python3
"""
Verify Fleet Data Pipeline setup: config, DB, Kafka, and topics.
Run from repo root: python scripts/verify.py
Does not require Kafka to be up for config check; reports status for each component.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    ok = True

    # 1. Config
    print("1. Config")
    try:
        from config import load_config
        cfg = load_config()
        k = cfg["kafka"]
        t = cfg["timescaledb"]
        print(f"   Kafka:          {k['bootstrap_servers']}")
        print(f"   Topics:         {list(k['topics'].values())}")
        print(f"   TimescaleDB:    {t['host']}:{t['port']} / {t['database']}")
        print("   OK")
    except Exception as e:
        print(f"   FAIL: {e}")
        ok = False

    # 2. TimescaleDB
    print("\n2. TimescaleDB")
    try:
        from config import load_config
        import psycopg2
        cfg = load_config()
        db = cfg["timescaledb"]
        conn = psycopg2.connect(
            host=db["host"],
            port=db["port"],
            dbname=db["database"],
            user=db["user"],
            password=db["password"],
            connect_timeout=3,
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'vehicle_telemetry'"
        )
        has_schema = cur.fetchone()[0] == 1
        cur.close()
        conn.close()
        if has_schema:
            print("   Connected, schema present (vehicle_telemetry exists)")
        else:
            print("   Connected, but schema not applied. Run sql/01_schema.sql against this DB.")
        print("   OK")
    except Exception as e:
        print(f"   Skip or FAIL: {e}")
        print("   (Start Docker: docker-compose up -d)")

    # 3. Kafka broker
    print("\n3. Kafka broker")
    try:
        from kafka import KafkaProducer
        from config import load_config
        cfg = load_config()
        bootstrap = cfg["kafka"]["bootstrap_servers"]
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            request_timeout_ms=3000,
        )
        producer.close()
        print(f"   {bootstrap} reachable")
        print("   OK")
    except Exception as e:
        print(f"   Skip or FAIL: {e}")
        print("   (Start Docker: docker-compose up -d, then create topics)")

    # 4. Kafka topics
    print("\n4. Kafka topics")
    try:
        from kafka.admin import KafkaAdminClient
        from config import load_config
        cfg = load_config()
        bootstrap = cfg["kafka"]["bootstrap_servers"]
        topics_wanted = list(cfg["kafka"]["topics"].values())
        client = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=3000)
        existing = set(client.list_topics())
        client.close()
        missing = [t for t in topics_wanted if t not in existing]
        if not missing:
            print(f"   All required topics present: {topics_wanted}")
            print("   OK")
        else:
            print(f"   Missing topics: {missing}")
            print("   Run: scripts/create_topics.ps1  or  scripts/create_topics.sh")
            ok = False
    except Exception as e:
        print(f"   Skip or FAIL: {e}")

    # 5. Python deps (optional)
    print("\n5. Python dependencies")
    for name in ["kafka", "psycopg2", "streamlit", "pandas", "plotly", "yaml"]:
        mod = "pyyaml" if name == "yaml" else name
        try:
            __import__(mod)
            print(f"   {mod} OK")
        except ImportError:
            print(f"   {mod} MISSING (pip install -r requirements.txt)")
            ok = False

    print()
    if ok:
        print("Verification passed. Next: run consumer, producer, dashboard (see README).")
    else:
        print("Some checks failed. Fix the items above and run verify again.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
