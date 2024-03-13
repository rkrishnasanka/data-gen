package main

import "database/sql"

/*
	SELECT n.nspname AS enum_schema,
	t.typname AS enum_name,
	e.enumlabel AS enum_value
	FROM pg_type t
	JOIN pg_enum e ON t.oid = e.enumtypid
	JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace;

*/

var enumTypesMemo = make(map[string]bool)

// CheckIfTypeIsEnum checks if the given column type is an enum type
func CheckIfTypeIsEnum(columnType string, db *sql.DB) bool {
	// If the result is in the memo, return it
	if result, ok := enumTypesMemo[columnType]; ok {
		return result
	}

	// Check if the column type given is an postgres enum type
	rows, err := db.Query(`
		SELECT DISTINCT
			t.typname AS enum_name  
		FROM pg_type t 
		JOIN pg_enum e ON t.oid = e.enumtypid  
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace;			
	`)
	if err != nil {
		panic(err)
	}

	if err := rows.Err(); err != nil {
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
		if typname == columnType {
			found = true
			break
		}
	}

	// Store the result in the memo
	enumTypesMemo[columnType] = found

	return found
}

// GetEnumOptions returns the enum options for the given column type
var enumValuesMemo = make(map[string][]string)

// GetEnumOptions returns the enum options for the given column type
func GetEnumOptions(columnType string, db *sql.DB) []string {
	// If the result is in the memo, return it
	if result, ok := enumValuesMemo[columnType]; ok {
		return result
	}

	ret := []string{}

	// Get the enum options for the given column type
	rows, err := db.Query(`
		SELECT DISTINCT
		e.enumlabel AS enum_value
		FROM pg_type t 
		JOIN pg_enum e ON t.oid = e.enumtypid  
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typname = $1;
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

	// Store the result in the memo
	enumValuesMemo[columnType] = ret

	return ret
}
