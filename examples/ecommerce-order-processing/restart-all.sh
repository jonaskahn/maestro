#!/bin/bash

echo "🛑 Stopping all containers..."
docker-compose down

echo "🧹 Removing any orphaned containers and networks..."
docker-compose down --remove-orphans

echo "🔥 Optionally remove volumes (uncomment if you want fresh data)..."
# docker-compose down -v  # Uncomment this line to remove all data volumes

echo "🚀 Starting all containers with new configuration..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 15

echo "🔍 Checking container status..."
docker-compose ps

echo "Health checks..."
echo " Kafka topics:"
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "🎉 All containers restarted successfully!"
echo "Kafka now uses gzip compression (KafkaJS compatible)"
echo "🚀 Ready to run your producer cronjob!"
