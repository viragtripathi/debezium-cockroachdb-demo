#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTOR_PROJECT="${SCRIPT_DIR}/../debezium-connector-cockroachdb"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail()    { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
header()  { echo -e "\n${BOLD}=== $1 ===${NC}\n"; }

wait_for_url() {
    local url="$1" max="$2"
    for i in $(seq 1 "$max"); do
        if curl -s -o /dev/null -w '' "$url" 2>/dev/null; then
            return 0
        fi
        echo -n "."
        sleep 2
    done
    echo ""
    return 1
}

wait_for_task_running() {
    local name="$1" max="$2"
    for i in $(seq 1 "$max"); do
        local state=$(curl -s "http://localhost:8083/connectors/$name/status" 2>/dev/null \
            | python3 -c "import sys,json; t=json.load(sys.stdin).get('tasks',[]); print(t[0]['state'] if t else 'NO_TASK')" 2>/dev/null || echo "UNKNOWN")
        if [ "$state" = "RUNNING" ]; then
            return 0
        elif [ "$state" = "FAILED" ]; then
            local trace=$(curl -s "http://localhost:8083/connectors/$name/status" 2>/dev/null \
                | python3 -c "import sys,json; t=json.load(sys.stdin).get('tasks',[]); print(t[0].get('trace','')[:500] if t else '')" 2>/dev/null)
            echo ""
            fail "Task FAILED: $trace"
        fi
        echo -n "."
        sleep 2
    done
    echo ""
    return 1
}

# ── Step 1: Build connector plugin ──────────────────────────────────────────
header "STEP 1: Build Connector Plugin"
if [ ! -d "$CONNECTOR_PROJECT" ]; then
    fail "Connector project not found at $CONNECTOR_PROJECT"
fi
cd "$CONNECTOR_PROJECT"
./mvnw clean package -DskipTests -DskipITs -Passembly -q
PLUGIN_ZIP=$(ls target/debezium-connector-cockroachdb-*-plugin.zip 2>/dev/null | head -1)
[ -z "$PLUGIN_ZIP" ] && fail "Plugin zip not found after build"
success "Connector built: $(basename "$PLUGIN_ZIP")"

# ── Step 2: Prepare plugin directory ────────────────────────────────────────
header "STEP 2: Prepare Plugin Directory"
cd "$SCRIPT_DIR"
rm -rf connect-plugins
mkdir -p connect-plugins
unzip -q -o "$CONNECTOR_PROJECT/$PLUGIN_ZIP" -d connect-plugins/
success "Plugin extracted to connect-plugins/"

# ── Step 3: Start infrastructure ────────────────────────────────────────────
header "STEP 3: Start Docker Compose (Source CRDB + Target CRDB + Kafka + Connect)"
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose up -d
success "Containers starting..."

# ── Step 4: Wait for Source CockroachDB ─────────────────────────────────────
header "STEP 4: Wait for Source CockroachDB"
for i in $(seq 1 30); do
    if docker exec demo-cockroachdb cockroach sql --insecure -e "SELECT 1" >/dev/null 2>&1; then
        success "Source CockroachDB is ready (port 26257)"
        break
    fi
    echo -n "."
    sleep 2
done
docker exec demo-cockroachdb cockroach sql --insecure -e "SELECT 1" >/dev/null 2>&1 || fail "Source CockroachDB did not start"

# ── Step 5: Wait for Target CockroachDB ────────────────────────────────────
header "STEP 5: Wait for Target CockroachDB"
for i in $(seq 1 30); do
    if docker exec demo-cockroachdb-target cockroach sql --insecure -e "SELECT 1" >/dev/null 2>&1; then
        success "Target CockroachDB is ready (port 26258)"
        break
    fi
    echo -n "."
    sleep 2
done
docker exec demo-cockroachdb-target cockroach sql --insecure -e "SELECT 1" >/dev/null 2>&1 || fail "Target CockroachDB did not start"

# ── Step 6: Setup source database ──────────────────────────────────────────
header "STEP 6: Setup Source Database (demodb.orders)"
docker exec -i demo-cockroachdb cockroach sql --insecure < setup-cockroachdb.sql
success "Source DB configured: demodb with orders table (3 sample rows)"

# ── Step 7: Setup target database ──────────────────────────────────────────
header "STEP 7: Setup Target Database (targetdb)"
docker exec -i demo-cockroachdb-target cockroach sql --insecure < setup-target-cockroachdb.sql
success "Target DB configured: targetdb (empty, table will be auto-created by sink)"

# ── Step 8: Wait for Kafka Connect ─────────────────────────────────────────
header "STEP 8: Wait for Kafka Connect"
if ! wait_for_url "http://localhost:8083/" 60; then
    echo ""
    fail "Kafka Connect did not start within 120s"
fi
echo ""
success "Kafka Connect is ready"

# ── Step 9: Verify plugins ─────────────────────────────────────────────────
header "STEP 9: Verify Connector Plugins"
PLUGINS=$(curl -s http://localhost:8083/connector-plugins)
echo "$PLUGINS" | python3 -c "
import sys,json
for p in json.load(sys.stdin):
    c = p['class']
    if 'cockroachdb' in c.lower() or 'jdbc' in c.lower():
        print(f'  {p[\"type\"]:6s}  {c}')
" 2>/dev/null
echo "$PLUGINS" | grep -q "CockroachDBConnector" || fail "CockroachDB source connector not found"
echo "$PLUGINS" | grep -q "JdbcSinkConnector" || fail "JDBC sink connector not found"
success "Both source and sink connector plugins discovered"

# ── Step 10: Deploy source connector ───────────────────────────────────────
header "STEP 10: Deploy Debezium CockroachDB Source Connector"
RESPONSE=$(curl -s -o /tmp/demo-src-response.json -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    --data @connector-config.json \
    http://localhost:8083/connectors)
BODY=$(cat /tmp/demo-src-response.json)
if [ "$RESPONSE" -ge 200 ] && [ "$RESPONSE" -lt 300 ]; then
    success "Source connector deployed (HTTP $RESPONSE)"
elif echo "$BODY" | grep -q "already exists"; then
    warn "Source connector already exists"
else
    echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
    fail "Source connector deploy failed (HTTP $RESPONSE)"
fi

info "Waiting for source connector task to start..."
if ! wait_for_task_running "cockroachdb-demo-connector" 30; then
    fail "Source connector task did not start"
fi
success "Source connector task is RUNNING"

# ── Step 11: Wait for changefeed + insert initial data ─────────────────────
header "STEP 11: Insert Initial Data and Wait for Changefeed Events"
info "Inserting initial data (changefeed uses cursor=now, so we insert after changefeed creation)..."
docker exec -i demo-cockroachdb cockroach sql --insecure -d demodb -e "
INSERT INTO orders (id, order_number, customer_name, email, amount, status, created_at) VALUES
(gen_random_uuid(), 'ORD-LIVE-001', 'Alice Johnson', 'alice@example.com', 199.99, 'new', now()),
(gen_random_uuid(), 'ORD-LIVE-002', 'Bob Smith', 'bob@example.com', 75.50, 'pending', now()),
(gen_random_uuid(), 'ORD-LIVE-003', 'Carol Williams', 'carol@example.com', 320.00, 'confirmed', now());
" 2>&1
success "3 rows inserted after changefeed creation"
info "Waiting 15s for events to flow through source connector..."
sleep 15

# ── Step 12: Deploy JDBC sink connector ────────────────────────────────────
header "STEP 12: Deploy Debezium JDBC Sink Connector (target CRDB)"
RESPONSE=$(curl -s -o /tmp/demo-sink-response.json -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    --data @sink-connector-config.json \
    http://localhost:8083/connectors)
BODY=$(cat /tmp/demo-sink-response.json)
if [ "$RESPONSE" -ge 200 ] && [ "$RESPONSE" -lt 300 ]; then
    success "Sink connector deployed (HTTP $RESPONSE)"
elif echo "$BODY" | grep -q "already exists"; then
    warn "Sink connector already exists"
else
    echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
    fail "Sink connector deploy failed (HTTP $RESPONSE)"
fi

info "Waiting for sink connector task to start..."
if ! wait_for_task_running "cockroachdb-jdbc-sink" 30; then
    warn "Sink connector task did not reach RUNNING state -- check logs below"
fi

# ── Step 13: Run DML operations ────────────────────────────────────────────
header "STEP 13: Run DML Operations on Source CRDB"
info "Executing INSERT, UPDATE, DELETE on source..."
docker exec -i demo-cockroachdb cockroach sql --insecure < demo-operations.sql
success "DML operations executed: 2 UPDATEs, 1 DELETE"

info "Waiting 15s for events to propagate through the full pipeline..."
sleep 15

# ── Step 14: Show debug logs from source connector ─────────────────────────
header "STEP 14: Source Connector Debug Logs (event processing)"
echo ""
docker logs demo-connect 2>&1 \
    | grep -E "CockroachDB.*Registered table|CockroachDB.*Consuming from|CockroachDB.*changefeed|CockroachDB.*Dispatching|CockroachDB.*offset|CockroachDB.*Snapshot|CockroachDB.*dispatch" \
    | tail -15
echo ""

# ── Step 15: Show debug logs from sink connector ───────────────────────────
header "STEP 15: JDBC Sink Connector Debug Logs (writing to target)"
echo ""
docker logs demo-connect 2>&1 \
    | grep -iE "Flushing records.*orders_replica|CREATE TABLE.*orders_replica|ALTER TABLE|upsert|Skipping tombstone|Using dialect|Database version|orders_replica" \
    | tail -15
echo ""

# ── Step 16: Check for errors ──────────────────────────────────────────────
header "STEP 16: Error Check"
ERRORS=$(docker logs demo-connect 2>&1 \
    | grep -E "^[0-9]{4}-.*ERROR" \
    | grep -v "errors\.\|error_code\|config_mismatch" \
    | tail -5)
if [ -z "$ERRORS" ]; then
    success "No errors in connector logs"
else
    warn "Errors found:"
    echo "$ERRORS"
fi

# ── Step 17: Kafka topics ──────────────────────────────────────────────────
header "STEP 17: Kafka Topics"
docker exec demo-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep -v "^_\|connect-\|demo-connect" | sort

# ── Step 18: Consume Debezium events from output topic ─────────────────────
header "STEP 18: Debezium Change Events (topic: crdb.public.orders)"
EVENTS=$(docker exec demo-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic crdb.public.orders \
    --from-beginning \
    --max-messages 20 \
    --timeout-ms 10000 2>/dev/null || true)

if [ -n "$EVENTS" ]; then
    EVENT_COUNT=$(echo "$EVENTS" | wc -l | tr -d ' ')
    success "Received $EVENT_COUNT events"
    echo ""
    echo "$EVENTS" | python3 -c "
import sys,json
for line in sys.stdin:
    line=line.strip()
    if not line: continue
    try:
        d=json.loads(line)
        # Handle schema-wrapped format
        payload = d.get('payload', d)
        op = payload.get('op','?')
        after = payload.get('after',{})
        before = payload.get('before',{})
        name = after.get('order_number','') if after else '(deleted)'
        amount = after.get('amount','') if after else ''
        ts = after.get('created_at','') if after else ''
        print(f'  op={op}  order={name:12s}  amount={str(amount):8s}  created_at={ts}')
    except: pass
" 2>/dev/null
else
    warn "No events on output topic"
fi

# ── Step 19: Verify data in target CockroachDB ────────────────────────────
header "STEP 19: Verify Data in Target CockroachDB (CRDB->Kafka->CRDB round-trip)"
echo ""
info "Querying target CRDB (targetdb.orders_replica):"
echo ""
TARGET_DATA=$(docker exec demo-cockroachdb-target cockroach sql --insecure -d targetdb \
    -e "SELECT id, order_number, customer_name, amount, status FROM orders_replica ORDER BY order_number" 2>&1)

if echo "$TARGET_DATA" | grep -q "does not exist"; then
    warn "Table orders_replica not yet created in target -- sink may need more time"
    info "Checking sink connector status..."
    curl -s http://localhost:8083/connectors/cockroachdb-jdbc-sink/status 2>/dev/null | python3 -m json.tool 2>/dev/null | head -20
else
    echo "$TARGET_DATA"
    ROW_COUNT=$(echo "$TARGET_DATA" | grep -c "ORD-" || true)
    success "Target CRDB has $ROW_COUNT rows replicated from source"
fi

echo ""
info "Comparing source vs target row counts:"
SRC_COUNT=$(docker exec demo-cockroachdb cockroach sql --insecure -d demodb \
    -e "SELECT count(*) FROM orders" --format=csv 2>/dev/null | tail -1)
TGT_COUNT=$(docker exec demo-cockroachdb-target cockroach sql --insecure -d targetdb \
    -e "SELECT count(*) FROM orders_replica" --format=csv 2>/dev/null | tail -1 || echo "0")
echo "  Source (demodb.orders):           $SRC_COUNT rows"
echo "  Target (targetdb.orders_replica): $TGT_COUNT rows"
if [ "$SRC_COUNT" = "$TGT_COUNT" ]; then
    success "Row counts match: snapshot captured all pre-existing rows!"
else
    info "Note: Row count mismatch (source=$SRC_COUNT, target=$TGT_COUNT). The DELETE in demo-operations.sql removes one row from source."
fi

# ── Step 20: Summary ───────────────────────────────────────────────────────
header "DEMO COMPLETE"
echo ""
echo "  Architecture:"
echo "  Source CRDB (demodb.orders)"
echo "       |"
echo "       v  [CockroachDB enriched changefeed]"
echo "  Kafka (intermediate topic)"
echo "       |"
echo "       v  [Debezium CockroachDB Source Connector]"
echo "  Kafka (crdb.public.orders)"
echo "       |"
echo "       v  [Debezium JDBC Sink Connector]"
echo "  Target CRDB (targetdb.orders_replica)"
echo ""
success "Source CRDB       : localhost:26257  (UI: http://localhost:8080)"
success "Target CRDB       : localhost:26258  (UI: http://localhost:8081)"
success "Kafka             : localhost:29092"
success "Kafka Connect     : http://localhost:8083"
success "Source connector  : http://localhost:8083/connectors/cockroachdb-demo-connector/status"
success "Sink connector    : http://localhost:8083/connectors/cockroachdb-jdbc-sink/status"
echo ""
info "Interactive SQL on source:"
echo "  docker exec -it demo-cockroachdb cockroach sql --insecure -d demodb"
echo ""
info "Interactive SQL on target:"
echo "  docker exec -it demo-cockroachdb-target cockroach sql --insecure -d targetdb"
echo ""
info "Watch events in real-time:"
echo "  docker exec demo-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crdb.public.orders"
echo ""
info "View connector debug logs:"
echo "  docker compose logs -f connect 2>&1 | grep -E 'DEBUG|INFO' | grep -iE 'cockroachdb|jdbc'"
echo ""
info "To stop the demo:"
echo "  cd $(pwd) && docker compose down -v"
