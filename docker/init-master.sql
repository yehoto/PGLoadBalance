CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';
SELECT pg_create_physical_replication_slot('phys_slot');