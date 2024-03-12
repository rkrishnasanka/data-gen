package main

import "database/sql"

func CheckIfTypeIsEnum(columnType string, db *sql.DB) bool {
	// Check if the column type given is an postgres enum type
	rows, err := db.Query(`

		SELECT t.typname
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'public'
		AND t.typname = $1;
	`, columnType)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// Iterate over the rows and check if the column type is an enum
	found := false
	for rows.Next() {
		var typname string
		if err := rows.Scan(&typname); err != nil {
			panic(err)
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return found
}

func GetEnumOptions(columnType string, db *sql.DB) []string {

	ret := []string{}

	// Get the enum options for the given column type
	rows, err := db.Query(`

		SELECT e.enumlabel
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'public'
		AND t.typname = $1;
	`, columnType)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// Iterate over the rows and get the enum options
	for rows.Next() {
		var enumlabel string
		if err := rows.Scan(&enumlabel); err != nil {
			panic(err)
		}
		ret = append(ret, enumlabel)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return ret

}
