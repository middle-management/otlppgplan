#!/bin/bash
# Quick demo script for PostgreSQL auto_explain to OTLP traces

set -e

echo "🚀 Starting PostgreSQL auto_explain → OTLP Traces Demo"
echo ""

# Start services
echo "📦 Starting Docker services..."
docker compose up -d --build

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 5

# Wait for PostgreSQL
echo "   Waiting for PostgreSQL..."
until docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; do
    sleep 1
done

# Wait for collectors
echo "   Waiting for collectors..."
sleep 3

echo ""
echo "✅ All services ready!"
echo ""

# Show service status
echo "📊 Service Status:"
docker compose ps
echo ""

# Run test queries
echo "🔍 Running test queries..."
docker compose exec -T postgres psql -U postgres -d testdb < test-queries.sql
echo ""

echo "✨ Demo is running!"
echo ""
echo "📍 Access points:"
echo "   - Jaeger UI: http://localhost:16686"
echo "   - PostgreSQL: localhost:5432 (user: postgres, password: postgres, db: testdb)"
echo ""
echo "💡 Try running more queries:"
echo "   docker compose exec postgres psql -U postgres -d testdb"
echo "   > SELECT * FROM users LIMIT 10;"
echo ""
echo "📝 View logs:"
echo "   docker compose logs -f otel-collector-pgplan"
echo ""
echo "🛑 Stop the demo:"
echo "   docker compose down"
echo ""
