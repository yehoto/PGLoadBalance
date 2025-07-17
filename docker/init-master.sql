-- Create replication user if not exists
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'replicator') THEN
    CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';
  END IF;
END $$;

-- Connect to database
\c postgres

-- Grant necessary permissions
GRANT SELECT ON pg_replication_slots TO replicator;
ALTER USER replicator WITH SUPERUSER;

-- Configure pg_hba.conf using shell commands
\! echo "host replication replicator 172.18.0.0/16 scram-sha-256" >> /var/lib/postgresql/data/pg_hba.conf
\! echo "host all all 172.18.0.0/16 scram-sha-256" >> /var/lib/postgresql/data/pg_hba.conf

-- Reload configuration
SELECT pg_reload_conf();

-- Create replication slots if they don't exist
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = 'phys_slot') THEN
    PERFORM pg_create_physical_replication_slot('phys_slot', true);
  END IF;
  
  IF NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = 'logic_slot') THEN
    PERFORM pg_create_logical_replication_slot('logic_slot', 'pgoutput');
  END IF;
END $$;

-- Example table
CREATE TABLE IF NOT EXISTS public.mytable (
    id SERIAL PRIMARY KEY,
    data TEXT
);