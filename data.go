package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"

	"github.com/go-faker/faker/v4"
)

// Fills the tables with sample data
func fillTables(rootNodes []*TableNode, db *sql.DB) {
	// Iterate over the root nodes and fill the tables
	fmt.Println("\nFilling Tables for a new traversal:")
	for _, rootNode := range rootNodes {
		fmt.Println("Filling table:", rootNode.TableName)
		fillTable(rootNode, db, 10)
	}
}

// Generate a sample entry for the table
func generateSampleEntryData(tableNode *TableNode, db *sql.DB) (map[string]interface{}, map[string]string) {

	// TODO: Get the dependent values from the parent tables
	depValues := getDepValues(tableNode, db)

	// Generate test values to insert into the table
	sampleValues := make(map[string]interface{})
	sampleTypes := make(map[string]string)

	// Iterate over the columns and generate test values
	for _, column := range tableNode.Columns {
		// SKip id columns
		if column.ColumnName == "id" {
			continue
		}

		// If the column is a dependent column, get the value from the dependent values
		if value, ok := depValues[column.ColumnName]; ok {
			sampleValues[column.ColumnName] = value
			sampleTypes[column.ColumnName] = column.DataType

			fmt.Println("Filling column:", column.ColumnName, "with value:", value, "(dependent column)")
			continue
		}

		// First check if the column is an enum
		if CheckIfTypeIsEnum(column.DataType, db) {
			// Print the column name and data type
			// Get the enum values
			enumOptions := GetEnumOptions(column.DataType, db)
			// Randomly select an enum value
			sampleValues[column.ColumnName] = enumOptions[rand.Intn(len(enumOptions))]
			sampleTypes[column.ColumnName] = column.DataType

			//Print the column name and data type
			fmt.Println("Filling column:", column.ColumnName, "with value:", sampleValues[column.ColumnName], "(enum)")
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
			case "phone_number":
				sampleValues[column.ColumnName] = faker.E164PhoneNumber()
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

		fmt.Println("Filling column:", column.ColumnName, "with value:", sampleValues[column.ColumnName])
	}

	return sampleValues, sampleTypes

}

// Fills the table with sample data, given the number of entries to fill
func fillTable(tableNode *TableNode, db *sql.DB, numEntries int) {
	fmt.Println("Filling table:", tableNode.TableName)

	for i := 0; i < numEntries; i++ {

		fmt.Println("Filling entry:", i)

		// Generate test values to insert into the table
		sampleValues, sampleTypes := generateSampleEntryData(tableNode, db)

		insertColumnNames := ""

		insertColumnValues := ""

		// Get the column names from the table node
		for columnName, value := range sampleValues {
			// Skip if the column is an id column
			if columnName == "id" {
				continue
			}

			insertColumnNames += columnName + ", "

			// Add the value to the insert statement
			insertColumnValues = formatAppendValue(sampleTypes, columnName, insertColumnValues, value)
		}

		// Remove the trailing comma and space
		insertColumnNames = insertColumnNames[:len(insertColumnNames)-2]
		insertColumnValues = insertColumnValues[:len(insertColumnValues)-2]

		// Construct and SQL insert statement to insert the sample data
		insertStatement := fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s );", tableNode.TableName, insertColumnNames, insertColumnValues)

		fmt.Println("Insert Statement:", insertStatement)

		// Execute the insert statement
		_, err := db.Exec(insertStatement)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func formatAppendValue(sampleTypes map[string]string, columnName string, insertStatement string, value interface{}) string {
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
	return insertStatement
}

// Get the dependent values from the parent tables
func getDepValues(tableNode *TableNode, db *sql.DB) map[string]interface{} {

	// Create the returning map
	depValues := make(map[string]interface{})

	// Iterate over the forgien nodes and get the dependent values
	for _, parent := range tableNode.ParentRelationships {

		// Get the parent table name
		parentTableName := parent.ParentTable
		parentColumnName := parent.ParentColumn
		chilColumnName := parent.ChildColumn

		// Construct the SQL query to select a random row from the parent table
		selectQuery := fmt.Sprintf("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1;", parentColumnName, parentTableName)

		// Print the select query
		fmt.Println("Select Query:", selectQuery)

		// Randomly select the row from the parent table
		rows, err := db.Query(selectQuery)

		if err != nil {
			panic(err)
		}

		defer rows.Close()

		// Get the column value
		var columnValue interface{}
		if !rows.Next() {
			panic("No rows in result set")
		}

		rows.Scan(&columnValue) // Add the column value to the dependent values
		depValues[chilColumnName] = columnValue

	}

	return depValues
}
