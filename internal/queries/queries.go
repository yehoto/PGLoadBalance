package queries

import "fmt"

const (
	CreateExtensionPgstattuple = `CREATE EXTENSION IF NOT EXISTS pgstattuple`

	DisableAutovacuumTemplate = `ALTER TABLE %s SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)`
	CreateTableTemplate       = `CREATE TABLE IF NOT EXISTS %s (id bigserial PRIMARY KEY, data text)`

	VacuumTemplate     = `VACUUM %s`
	VacuumFullTemplate = `VACUUM FULL %s`

	InsertTemplate = `INSERT INTO %s(data) VALUES($1)`
	DeleteTemplate = `DELETE FROM %s WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)`
	UpdateTemplate = `UPDATE %s SET data = $1 WHERE id IN (SELECT id FROM %s ORDER BY random() LIMIT %d)`
)

func DisableAutovacuum(tableName string) string {
	return fmt.Sprintf(DisableAutovacuumTemplate, tableName)
}

func CreateTable(tableName string) string {
	return fmt.Sprintf(CreateTableTemplate, tableName)
}

func Vacuum(tableName string) string {
	return fmt.Sprintf(VacuumTemplate, tableName)
}

func VacuumFull(tableName string) string {
	return fmt.Sprintf(VacuumFullTemplate, tableName)
}

func Insert(tableName string) string {
	return fmt.Sprintf(InsertTemplate, tableName)
}

func Delete(tableName string, limit int) string {
	return fmt.Sprintf(DeleteTemplate, tableName, tableName, limit)
}

func Update(tableName string, limit int) string {
	return fmt.Sprintf(UpdateTemplate, tableName, tableName, limit)
}
