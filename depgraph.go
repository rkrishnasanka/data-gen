package main

import (
	"github.com/golang-collections/collections/stack"
)

// Create a dependency tree to capture what tables are dependent on what other tables
type TableNode struct {
	TableName           string
	Columns             []*TableColumn
	Children            []*TableNode
	ParentRelationships []*ForeignKeyConstraint
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

	// Add the foreign key constraint to the parent and child nodes
	child.ParentRelationships = append(child.ParentRelationships, &ForeignKeyConstraint{
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

func generateFillOrder(tableNodes map[string]*TableNode) []*TableNode {
	// Identify the root nodes
	fillOrder := make([]*TableNode, 0)

	rootNodes := make([]*TableNode, 0)

	// Identify the root nodes
	identifyRoots(&rootNodes, tableNodes)

	// Toplogically sort each of the root nodes

	// Create a map to keep track of the visited nodes
	visited := make(map[*TableNode]bool)

	// Iterate over the root nodes and topologically sort each of them
	for _, rootNode := range rootNodes {

		// Create a stack
		stack := stack.New()
		dfsTraverse(rootNode, &fillOrder, visited, stack)

		// Pop the stack and put it into the fill order
		for stack.Len() > 0 {
			fillOrder = append(fillOrder, stack.Pop().(*TableNode))
		}

		reverse(fillOrder)

	}

	return fillOrder
}

func dfsTraverse(tableNode *TableNode, fillOrder *[]*TableNode, visited map[*TableNode]bool, stack *stack.Stack) {
	// Mark the current node as visited
	visited[tableNode] = true

	// Look thorugh the children of the current node and traverse if they haven't been visited
	for _, child := range tableNode.Children {
		if !visited[child] {
			dfsTraverse(child, fillOrder, visited, stack)
		}
	}

	// Push the current node to the stack
	stack.Push(tableNode)
}

func reverse(nodes []*TableNode) {
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

// func topologicalSort(tableNode1 *TableNode, tableNode2 *[]*TableNode, visited map[*TableNode]bool) {
// 	// Mark the current node as visited
// 	visited[tableNode1] = true

// 	// Recur for all the children of the current node
// 	for _, child := range tableNode1.Children {
// 		if !visited[child] {
// 			topologicalSort(child, tableNode2, visited)
// 		}
// 	}

// 	// Push the current node to the stack
// 	*tableNode2 = append(*tableNode2, tableNode1)
// }
