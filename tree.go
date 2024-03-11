package main

// Create a dependency tree to capture what tables are dependent on what other tables
type TableNode struct {
	TableName             string
	Columns               []*TableColumn
	Children              []*TableNode
	Parents               []*TableNode
	ForeignKeyConstraints []*ForeignKeyConstraint
}

// Create a struct to capture the column name and data type
type TableColumn struct {
	ColumnName string
	DataType   string
}

// Create a struct to capture the foreign key constraint
type ForeignKeyConstraint struct {
	ConstraintName string
	ParentTable    string
	ParentColumn   string
	ChildColumn    string
}

// Add a method to the TableNode struct to add a child node
func (t *TableNode) AddChild(child *TableNode, constraintName string, parentColumn string, childColumn string) {
	t.Children = append(t.Children, child)
	child.Parents = append(child.Parents, t)

	// Add the foreign key constraint to the parent and child nodes
	child.ForeignKeyConstraints = append(child.ForeignKeyConstraints, &ForeignKeyConstraint{
		ConstraintName: constraintName,
		ParentTable:    t.TableName,
		ParentColumn:   parentColumn,
		ChildColumn:    childColumn,
	})

}

// Add a method to the TableNode struct to add a column
func (t *TableNode) AddColumn(column *TableColumn) {
	t.Columns = append(t.Columns, column)
}

// Add a method to the TableNode struct to create a new TableNode
func (t *TableNode) CreateTableNode(tableName string) *TableNode {
	return &TableNode{
		TableName: tableName,
	}
}

func generateTopologicalSort(tableNodes map[string]*TableNode) []*TableNode {
	var stack []*TableNode
	visited := make(map[*TableNode]bool)

	// Iterate over the table nodes and identify the roots
	for _, tableNode := range tableNodes {
		if len(tableNode.Parents) == 0 {
			// Start DFS from the root node
			topologicalSort(tableNode, &stack, visited)
		}
	}

	// Reverse the stack to get the topological order
	reverse(stack)

	return stack
}

func reverse(stack []*TableNode) {
	for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
		stack[i], stack[j] = stack[j], stack[i]
	}
}

func topologicalSort(tableNode1 *TableNode, tableNode2 *[]*TableNode, visited map[*TableNode]bool) {
	// Mark the current node as visited
	visited[tableNode1] = true

	// Recur for all the children of the current node
	for _, child := range tableNode1.Children {
		if !visited[child] {
			topologicalSort(child, tableNode2, visited)
		}
	}

	// Push the current node to the stack
	*tableNode2 = append(*tableNode2, tableNode1)
}
