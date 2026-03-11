package db

func TablesSQL(filtered bool) string {
	if filtered {
		return `
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = $1
ORDER BY table_schema, table_name;
`
	}

	return `
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
`
}

func DescribeTableSQL() string {
	return `
SELECT
    c.column_name,
    c.data_type,
    c.is_nullable,
    COALESCE(c.column_default, '') AS column_default,
    COALESCE(pgd.description, '') AS comment
FROM information_schema.columns c
LEFT JOIN pg_catalog.pg_statio_all_tables st
    ON c.table_schema = st.schemaname AND c.table_name = st.relname
LEFT JOIN pg_catalog.pg_description pgd
    ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
WHERE c.table_schema = current_schema() AND c.table_name = $1
ORDER BY c.ordinal_position;
`
}

func ColumnConstraintsSQL() string {
	return `
SELECT
    c.column_name,
    c.data_type,
    c.is_nullable,
    COALESCE(c.column_default, '') AS column_default,
    COALESCE(string_agg(DISTINCT tc.constraint_type, ', '), '') AS constraints,
    COALESCE(pgd.description, '') AS comment
FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage kcu
    ON c.table_schema = kcu.table_schema
    AND c.table_name = kcu.table_name
    AND c.column_name = kcu.column_name
LEFT JOIN information_schema.table_constraints tc
    ON kcu.constraint_schema = tc.constraint_schema
    AND kcu.constraint_name = tc.constraint_name
LEFT JOIN pg_catalog.pg_statio_all_tables st
    ON c.table_schema = st.schemaname AND c.table_name = st.relname
LEFT JOIN pg_catalog.pg_description pgd
    ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
WHERE c.table_schema = current_schema() AND c.table_name = $1
GROUP BY
    c.ordinal_position,
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.column_default,
    pgd.description
ORDER BY c.ordinal_position;
`
}

func UsersSQL() string {
	return `
SELECT
    rolname,
    rolsuper,
    rolcreaterole,
    rolcreatedb,
    rolcanlogin,
    rolreplication,
    rolconnlimit
FROM pg_roles
ORDER BY rolname;
`
}

func IndexesSQL(filtered bool) string {
	if filtered {
		return `
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = current_schema() AND tablename = $1
ORDER BY schemaname, tablename, indexname;
`
	}

	return `
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename, indexname;
`
}

func SizesSQL() string {
	return `
SELECT
    schemaname || '.' || relname AS table,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_relation_size(relid)) AS data_size,
    pg_size_pretty(pg_indexes_size(relid)) AS index_size,
    n_live_tup AS row_estimate
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT $1;
`
}

func FKeysSQL() string {
	return `
SELECT
    conname AS constraint_name,
    src_ns.nspname || '.' || src.relname AS table_from,
    a.attname AS column_from,
    ref_ns.nspname || '.' || ref.relname AS table_to,
    af.attname AS column_to
FROM pg_constraint c
JOIN pg_class src ON src.oid = c.conrelid
JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
JOIN pg_class ref ON ref.oid = c.confrelid
JOIN pg_namespace ref_ns ON ref_ns.oid = ref.relnamespace
JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
JOIN pg_attribute af ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid
WHERE c.contype = 'f' AND src_ns.nspname = current_schema() AND src.relname = $1;
`
}

func TablePrivilegesSQL() string {
	return `
SELECT
    grantee,
    privilege_type,
    is_grantable,
    with_hierarchy
FROM information_schema.role_table_grants
WHERE table_schema = current_schema() AND table_name = $1
ORDER BY grantee, privilege_type;
`
}

func ActivitySQL() string {
	return `
SELECT pid, usename, application_name, client_addr,
       state, query_start, left(query, 80) AS query
FROM pg_stat_activity
WHERE state IS NOT NULL
ORDER BY query_start DESC;
`
}

func LocksSQL() string {
	return `
SELECT
    blocked.pid AS blocked_pid,
    left(blocked.query, 80) AS blocked_query,
    blocking.pid AS blocking_pid,
    left(blocking.query, 80) AS blocking_query
FROM pg_locks bl
JOIN pg_stat_activity blocked ON bl.pid = blocked.pid
JOIN pg_locks kl ON bl.locktype = kl.locktype
    AND bl.relation IS NOT DISTINCT FROM kl.relation
    AND bl.pid != kl.pid
JOIN pg_stat_activity blocking ON kl.pid = blocking.pid
WHERE NOT bl.granted;
`
}

func EnumsSQL() string {
	return `
SELECT
    n.nspname AS schema,
    t.typname AS enum_name,
    e.enumlabel AS value
FROM pg_type t
JOIN pg_enum e ON t.oid = e.enumtypid
JOIN pg_namespace n ON n.oid = t.typnamespace
ORDER BY n.nspname, t.typname, e.enumsortorder;
`
}

func ConnectionTestSQL() string {
	return `
SELECT
    version() AS version,
    pg_size_pretty(pg_database_size(current_database())) AS database_size,
    (
        SELECT count(*)
        FROM pg_stat_activity
        WHERE datname = current_database()
    ) AS active_connections;
`
}
