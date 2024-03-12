package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"

	"github.com/go-faker/faker/v4"
)

// Fills the tables with sample data
func fillTables(rootNodes []*TableNode, tableNodes map[string]*TableNode, db *sql.DB) {
	// Iterate over the root nodes and fill the tables
	fmt.Println("\nFilling Tables for a new traversal:")
	for _, rootNode := range rootNodes {
		fmt.Println("Filling table:", rootNode.TableName)
		fillTable(rootNode, tableNodes, db, 10)
	}
}

// Generate a sample entry for the table
func generateSampleEntry(tableNode *TableNode, depValues map[string]interface{}) (map[string]interface{}, map[string]string) {
	// Generate test values to insert into the table
	sampleValues := make(map[string]interface{})
	sampleTypes := make(map[string]string)

	// Iterate over the columns and generate test values
	for _, column := range tableNode.Columns {
		// SKip id columns
		if column.ColumnName == "id" {
			continue
		}

		fmt.Println("Filling column:", column.ColumnName, "with type:", column.DataType)
		sampleTypes[column.ColumnName] = column.DataType

		// Generate sample data based on the data type
		switch column.DataType {
		case "bigint":
			sampleValues[column.ColumnName] = rand.Int63()
		case "int":
			sampleValues[column.ColumnName] = rand.Intn(100)
		case "text":

			switch column.ColumnName {
			case "first_name":
				sampleValues[column.ColumnName] = faker.FirstName()
			case "last_name":
				sampleValues[column.ColumnName] = faker.LastName()
			case "email":
				sampleValues[column.ColumnName] = faker.Email()
			case "name":
				sampleValues[column.ColumnName] = faker.Word()
			default:
				sampleValues[column.ColumnName] = faker.Sentence()
			}

		case "date":
			sampleValues[column.ColumnName] = faker.Timestamp()
		case "boolean":
			sampleValues[column.ColumnName] = rand.Intn(2) == 1
		case "real":
			sampleValues[column.ColumnName] = rand.Float64()
		case "jsonb":
			sampleValues[column.ColumnName] = `{"key": "value"}`
		}
	}

	return sampleValues, sampleTypes

}

// Fills the table with sample data, given the number of entries to fill
func fillTable(tableNode *TableNode, tableNodes map[string]*TableNode, db *sql.DB, numEntries int) {
	fmt.Println("Filling table:", tableNode.TableName)

	for i := 0; i < numEntries; i++ {

		fmt.Println("Filling entry:", i)

		// TODO: Get the dependent values from the parent tables
		depValues := getDepValues(tableNode, tableNodes, db)

		// Generate test values to insert into the table
		sampleValues, sampleTypes := generateSampleEntry(tableNode, depValues)

		// Construct and SQL insert statement to insert the sample data
		insertStatement := fmt.Sprintf("INSERT INTO %s (", tableNode.TableName)

		// Get the column names from the table node
		for _, column := range tableNode.Columns {
			// Skip if the column is an id column
			if column.ColumnName == "id" {
				continue
			}

			insertStatement += column.ColumnName + ", "
		}

		insertStatement = insertStatement[:len(insertStatement)-2] + ") VALUES ("
		for columnName, value := range sampleValues {

			//Skip if the column is an id column
			if columnName == "id" {
				continue
			}

			// Add the value to the insert statement
			switch sampleTypes[columnName] {
			case "bigint":
				insertStatement += fmt.Sprintf("%d, ", value)
			case "int":
				insertStatement += fmt.Sprintf("%d, ", value)
			case "text":
				insertStatement += fmt.Sprintf("'%s', ", value)
			case "date":
				insertStatement += fmt.Sprintf("'%s', ", value)
			case "boolean":
				insertStatement += fmt.Sprintf("%t, ", value)
			case "real":
				insertStatement += fmt.Sprintf("%f, ", value)
			case "jsonb":
				insertStatement += fmt.Sprintf("'%s', ", value)
			}
		}

		insertStatement = insertStatement[:len(insertStatement)-2] + ");"

		fmt.Println("Insert Statement:", insertStatement)

		// Execute the insert statement
		_, err := db.Exec(insertStatement)
		if err != nil {
			log.Fatal(err)
		}
	}

}

// Get the dependent values from the parent tables
func getDepValues(tableNode *TableNode, tableNodes map[string]*TableNode, db *sql.DB) map[string]interface{} {

	// Create the returning map
	depValues := make(map[string]interface{})

	// Iterate over the forgien nodes and get the dependent values
	for _, parent := range tableNode.ParentRelationships {

		// Get the parent table name
		parentTableName := parent.ParentTable
		parentColumnName := parent.ParentColumn
		chilColumnName := parent.ChildColumn

		// Randomly select the row from the parent table
		rows, err := db.Query(fmt.Sprintf("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1;", parentColumnName, parentTableName))

		if err != nil {
			panic(err)
		}

		// Get the column value
		var columnValue interface{}
		for rows.Next() {
			rows.Scan(&columnValue)
		}

		// Add the column value to the dependent values
		depValues[chilColumnName] = columnValue

	}

	return depValues
}
