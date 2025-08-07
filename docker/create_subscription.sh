#!/bin/bash
set -e
echo "*************** Creating subscription after tables are ready ***************"
# Ждем, пока таблицы будут созданы на primary
echo "Waiting for tables to be created on primary server..."
for i in {1..60}; do
  if psql -h postgres-primary -U replicator -d appdb -tAc "SELECT 1 FROM pg_tables WHERE tablename = 'test_0'" > /dev/null 2>&1; then
    echo "Tables are ready on primary! Proceeding with subscription creation."
    break
  fi
  echo "Tables not ready yet. Waiting... ($i/60)"
  sleep 2
  if [ $i -eq 60 ]; then
    echo "ERROR: Tables never got created on primary"
    exit 1
  fi
done

# Проверяем существование подписки
if psql -h postgres-replica -U postgres -d appdb -tAc "SELECT 1 FROM pg_subscription WHERE subname = 'appdb_subscription'" | grep -q 1; then
  echo "Subscription already exists. Skipping creation."
else
  echo "Creating subscription to primary..."
  psql -h postgres-replica -U postgres -d appdb -c "
    CREATE SUBSCRIPTION appdb_subscription
    CONNECTION 'host=postgres-primary port=5432 dbname=appdb user=replicator password=replicator_pass'
    PUBLICATION appdb_publication
    WITH (copy_data = true);
  "
fi
echo "*************** Subscription status checked ***************"