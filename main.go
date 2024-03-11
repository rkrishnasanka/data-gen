package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

// Create a dependency tree to capture what tables are dependent on what other tables
const (
	host     = "localhost" // replace with your PostgreSQL host
	port     = 5432        // replace with your PostgreSQL port
	user     = "postgres"  // replace with your PostgreSQL user
	password = "postgres"  // replace with your PostgreSQL password
	dbname   = "postgres"  // replace with your PostgreSQL database name
)

// Create a dependency tree to capture what tables are dependent on what other tables
func generateDependencyTree(db *sql.DB, tableNodes map[string]*TableNode) {
	// Query to retrieve schema information
	rows, err := db.Query(`
		SELECT table_schema, table_name, column_name, data_type
		FROM information_schema.columns
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Iterate over the rows and print table names, column names and their types
	fmt.Println("Tables and Columns:")
	for rows.Next() {
		var schema, table, column, dataType string
		if err := rows.Scan(&schema, &table, &column, &dataType); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s.%s.%s: %s\n", schema, table, column, dataType)

		// Create a new TableNode if it doesn't exist
		if _, ok := tableNodes[table]; !ok {
			tableNodes[table] = &TableNode{
				TableName: table,
			}
		}

		// Add the column to the TableNode corresponding to the table
		tableNodes[table].AddColumn(&TableColumn{
			ColumnName: column,
			DataType:   dataType,
		})
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Query to retrieve foreign key information
	rows, err = db.Query(`
		SELECT
			tc.table_schema, tc.table_name, kcu.column_name, tc.constraint_name,
			ccu.table_schema AS foreign_table_schema,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name
		FROM

			information_schema.table_constraints AS tc
			JOIN information_schema.key_column_usage AS kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
			JOIN information_schema.constraint_column_usage AS ccu
				ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY';
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Iterate over the rows and print foreign key information
	fmt.Println("\nForeign Keys:")
	for rows.Next() {
		var schema, table, column, foreignSchema, foreignTable, foreignColumn, constraintName string
		if err := rows.Scan(&schema, &table, &column, &constraintName, &foreignSchema, &foreignTable, &foreignColumn); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s : %s.%s.%s -> %s.%s.%s\n", constraintName, schema, table, column, foreignSchema, foreignTable, foreignColumn)

		// Create a new TableNode if it doesn't exist
		if _, ok := tableNodes[table]; !ok {
			tableNodes[table] = &TableNode{
				TableName: table,
			}
		}

		// Create a new TableNode if it doesn't exist
		if _, ok := tableNodes[foreignTable]; !ok {
			tableNodes[foreignTable] = &TableNode{
				TableName: foreignTable,
			}
		}

		// Add the foreign table as a child of the table
		tableNodes[foreignTable].AddChild(tableNodes[table], "fk_constraint_name", foreignColumn, column)
	}

}

// Create a struct to capture the column name and data type
func identifyRoots(rootNodes *[]*TableNode, tableNodes map[string]*TableNode) {
	// Iterate over the table nodes and identify the roots
	for _, tableNode := range tableNodes {
		if len(tableNode.Parents) == 0 {
			*rootNodes = append(*rootNodes, tableNode)
		}
	}

	// Print the root nodes
	fmt.Println("\nRoot Nodes:")
	for _, rootNode := range *rootNodes {
		fmt.Println(rootNode.TableName)
	}
}

func fillTables(rootNodes []*TableNode, tableNodes map[string]*TableNode) {
	// Iterate over the root nodes and fill the tables
	fmt.Println("\nFilling Tables for a new traversal:")
	for _, rootNode := range rootNodes {
		fmt.Println("Filling table:", rootNode.TableName)
		fillTable(rootNode, tableNodes)
	}
}

func fillTable(tableNode *TableNode, tableNodes map[string]*TableNode) {
	fmt.Println("Filling table:", tableNode.TableName)
}

func main() {
	// Create the PostgreSQL connection string
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	// Connect to the PostgreSQL database
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	tableNodes := make(map[string]*TableNode)

	// Generate the dependency tree
	generateDependencyTree(db, tableNodes)

	traversals := generateTopologicalSort(tableNodes)

	fillTables(traversals, tableNodes)

}
