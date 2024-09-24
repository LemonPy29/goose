package dialectquery

import "fmt"

type Risingwave struct{}

var _ Querier = (*Postgres)(nil)

func (p *Risingwave) CreateTable(tableName string) string {
	q := `CREATE TABLE %s (
		id bigint primary key default extract(epoch from now()) * 1000,
		version_id bigint,
		is_applied boolean,
		tstamp timestamptz DEFAULT now()
	)`
	return fmt.Sprintf(q, tableName)
}

func (p *Risingwave) InsertVersion(tableName string) string {
	q := `INSERT INTO %s (version_id, is_applied) VALUES ($1, $2)`
	return fmt.Sprintf(q, tableName)
}

func (p *Risingwave) DeleteVersion(tableName string) string {
	q := `DELETE FROM %s WHERE version_id=$1`
	return fmt.Sprintf(q, tableName)
}

func (p *Risingwave) GetMigrationByVersion(tableName string) string {
	q := `SELECT tstamp, is_applied FROM %s WHERE version_id=$1 ORDER BY tstamp DESC LIMIT 1`
	return fmt.Sprintf(q, tableName)
}

func (p *Risingwave) ListMigrations(tableName string) string {
	q := `SELECT version_id, is_applied from %s ORDER BY version_id DESC`
	return fmt.Sprintf(q, tableName)
}

func (p *Risingwave) GetLatestVersion(tableName string) string {
	q := `SELECT max(version_id) FROM %s`
	return fmt.Sprintf(q, tableName)
}
