set -e

echo "*************** Initializing Replica ***************"

sleep 10

echo "Cleaning old data..."
rm -rf ${PGDATA:?}/*

echo "Starting base backup from primary..."
PGPASSWORD=${REPLICATOR_PASSWORD} pg_basebackup -h postgres-primary -D ${PGDATA} -U ${REPLICATOR_USER} --verbose --progress --wal-method=stream

echo "Configuring replica connection to primary..."
cat >> ${PGDATA}/postgresql.conf <<EOF

# --- Standby Configuration ---
primary_conninfo = 'host=postgres-primary port=5432 user=${REPLICATOR_USER} password=${REPLICATOR_PASSWORD} application_name=replica1'
primary_slot_name = 'replica1_slot'
hot_standby = on
# -----------------------------
EOF

echo "Creating standby.signal..."
touch ${PGDATA}/standby.signal

echo "*************** Replica Initialized ***************"