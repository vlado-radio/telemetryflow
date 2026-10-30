#!/usr/bin/env bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}=== LAUNCHING A TELEMETRY SYSTEM (Local Maven) ===${NC}"

# Using Maven from a folder inside the project
echo "🚀 Launching the consumer..."
./apache-maven-3.10.0/bin/mvn exec:java -Dexec.mainClass="com.mycompany.telemetryproducer.RadioDataConsumerUpdate" &
CONSUMER_PID=$!

if [ -z "$CONSUMER_PID" ] || ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
    echo -e "${RED}❌ Consumer startup error${NC}"
    exit 1
fi

echo "✅ The consumer is running (PID: $CONSUMER_PID)"
sleep 3

echo -e "${GREEN}--- Launching a producer ---${NC}"
echo "⚠️ Enter the data here."
./apache-maven-3.10.0/bin/mvn exec:java -Dexec.mainClass="com.mycompany.telemetryproducer.RadioDataProducerUpdate"

PRODUCER_EXIT_CODE=$?

echo ""
echo "🛑 Stopping the consumer..."
kill $CONSUMER_PID 2>/dev/null
wait $CONSUMER_PID 2>/dev/null || true

echo "✅ All processes are stopped."
exit $PRODUCER_EXIT_CODE
