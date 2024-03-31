package consistent_hashing

import (
	"errors"
	"testing"
)

func TestBinarySearchAdd(t *testing.T) {
	tests := []struct {
		nodes    []node
		search   uint32
		expected int
	}{
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 3, 2},  // Value found in the middle
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 6, 4},  // Value exceeds the maximum
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 0, -1}, // Value less than the minimum
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 1, 0},  // Value equals to the minimum
		{[]node{{1, "A"}, {2, "B"}, {4, "D"}, {5, "E"}}, 3, 1},            // Value not present, between two existing values
	}

	for i, test := range tests {
		hr := hashRing{nodes: test.nodes}
		result := hr.binarySearchAdd(test.search)
		if result != test.expected {
			t.Errorf("Test case %d failed: expected %d but got %d", i+1, test.expected, result)
		}
	}
}

func TestBinarySearchRemove(t *testing.T) {
	tests := []struct {
		nodes    []node
		remove   uint32
		expected int
	}{
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 3, 2},  // Value found in the middle
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 6, -1}, // Value exceeds the maximum
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 0, -1}, // Value less than the minimum
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 1, 0},  // Value equals to the minimum
		{[]node{{1, "A"}, {2, "B"}, {4, "D"}, {5, "E"}}, 3, -1},           // Value not present, between two existing values
	}

	for i, test := range tests {
		hr := hashRing{nodes: test.nodes}
		result := hr.binarySearchRemove(test.remove)
		if result != test.expected {
			t.Errorf("Test case %d failed: expected %d but got %d", i+1, test.expected, result)
		}
	}
}

func TestBinarySearchAssign(t *testing.T) {
	tests := []struct {
		nodes    []node
		assign   uint32
		expected int
	}{
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 3, 2}, // Value found in the middle
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 6, 0}, // Value exceeds the maximum
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 0, 0}, // Value less than the minimum
		{[]node{{1, "A"}, {2, "B"}, {3, "C"}, {4, "D"}, {5, "E"}}, 1, 0}, // Value equals to the minimum
		{[]node{{1, "A"}, {2, "B"}, {4, "D"}, {5, "E"}}, 3, 2},           // Value not present, between two existing values
	}

	for i, test := range tests {
		hr := hashRing{nodes: test.nodes}
		result := hr.binarySearchAssign(test.assign)
		if result != test.expected {
			t.Errorf("Test case %d failed: expected %d but got %d", i+1, test.expected, result)
		}
	}
}

func TestInitHashRing(t *testing.T) {
	tests := []struct {
		maxNodes          int
		replicationFactor int
	}{
		{10, 3},  // Typical case
		{0, 5},   // Edge case with maxNodes = 0
		{100, 0}, // Edge case with replicationFactor = 0
		{5, -1},  // Edge case with negative replicationFactor
	}

	for i, test := range tests {
		hr := InitHashRing(test.maxNodes, test.replicationFactor)

		if hr.maxNodes != test.maxNodes {
			t.Errorf("Test case %d failed: expected maxNodes %d but got %d", i+1, test.maxNodes, hr.maxNodes)
		}

		if hr.replicationFactor != test.replicationFactor {
			t.Errorf("Test case %d failed: expected replicationFactor %d but got %d", i+1, test.replicationFactor, hr.replicationFactor)
		}

		if len(hr.nodes) != 0 {
			t.Errorf("Test case %d failed: expected empty nodes slice but got non-empty slice", i+1)
		}
	}
}

func TestAddNode(t *testing.T) {
	testCases := []struct {
		maxNodes     int
		nodes        []node
		nodeToAdd    string
		expectedErr  error
		expectedNode string
		expectedNext string
	}{
		{ // Adding a node successfully
			maxNodes:     3,
			nodes:        []node{{key: 123, value: "node1"}, {key: 456, value: "node2"}},
			nodeToAdd:    "node3",
			expectedErr:  nil,
			expectedNode: "node3",
			expectedNext: "node1",
		},
		{ // Attempting to add a node when no slot is available
			maxNodes:    2,
			nodes:       []node{{key: 123, value: "node1"}, {key: 456, value: "node2"}},
			nodeToAdd:   "node3",
			expectedErr: errors.New("no slot available for node in hash ring"),
		},
		{ // Adding a node successfully when maxNodes is 1
			maxNodes:     1,
			nodes:        []node{},
			nodeToAdd:    "node1",
			expectedErr:  nil,
			expectedNode: "node1",
		},
		{ // Adding a node successfully when maxNodes is 2
			maxNodes:     2,
			nodes:        []node{{key: 123, value: "node1"}},
			nodeToAdd:    "node2",
			expectedErr:  nil,
			expectedNode: "node2",
		},
	}

	for i, testCase := range testCases {
		hr := hashRing{maxNodes: testCase.maxNodes, nodes: testCase.nodes}
		newNode, err := hr.addNode(testCase.nodeToAdd)

		if (err != nil && testCase.expectedErr == nil) || (err == nil && testCase.expectedErr != nil) {
			t.Errorf("Test case %d failed: expected error %v but got %v", i+1, testCase.expectedErr, err)
			continue
		}

		if err != nil && testCase.expectedErr != nil && err.Error() != testCase.expectedErr.Error() {
			t.Errorf("Test case %d failed: expected error %v but got %v", i+1, testCase.expectedErr, err)
			continue
		}

		if newNode.value != testCase.expectedNode {
			t.Errorf("Test case %d failed: expected node value %s but got %s", i+1, testCase.expectedNode, newNode.value)
		}
	}
}

func TestRemoveNode(t *testing.T) {
	testCases := []struct {
		nodes           []node
		nodeToRemove    string
		expectedErr     error
		expectedNodeKey uint32
	}{
		{ // Attempting to remove a node from an empty hash ring
			nodes:        []node{},
			nodeToRemove: "node1",
			expectedErr:  errors.New("empty hash ring"),
		},
		{ // Attempting to remove a non-existing node
			nodes:        []node{{key: 123, value: "node1"}, {key: 456, value: "node2"}},
			nodeToRemove: "node3",
			expectedErr:  errors.New("node doesn't exist"),
		},
	}

	for i, testCase := range testCases {
		hr := hashRing{nodes: testCase.nodes}
		removedNode, err := hr.removeNode(testCase.nodeToRemove)

		if (err != nil && testCase.expectedErr == nil) || (err == nil && testCase.expectedErr != nil) {
			t.Errorf("Test case %d failed: expected error %v but got %v", i+1, testCase.expectedErr, err)
			continue
		}

		if err != nil && testCase.expectedErr != nil && err.Error() != testCase.expectedErr.Error() {
			t.Errorf("Test case %d failed: expected error %v but got %v", i+1, testCase.expectedErr, err)
			continue
		}

		if removedNode != nil && removedNode.key != testCase.expectedNodeKey {
			t.Errorf("Test case %d failed: expected removed node key %d but got %d", i+1, testCase.expectedNodeKey, removedNode.key)
		}
	}
}

func TestAssignNode(t *testing.T) {
	testCases := []struct {
		nodes          []node
		value          string
		expectedNode   *node
		expectedErr    error
	}{
		{ // Assigning a node successfully
			nodes:        []node{{key: 123, value: "node1"}, {key: 456, value: "node2"}},
			value:        "node1",
			expectedNode: &node{key: 123, value: "node1"},
			expectedErr:  nil,
		},
	}

	for i, testCase := range testCases {
		hr := hashRing{nodes: testCase.nodes}
		assignedNode, err := hr.assignNode(testCase.value)

		if (err != nil && testCase.expectedErr == nil) || (err == nil && testCase.expectedErr != nil) {
			t.Errorf("Test case %d failed: expected error %v but got %v", i+1, testCase.expectedErr, err)
			continue
		}

		if err != nil && testCase.expectedErr != nil && err.Error() != testCase.expectedErr.Error() {
			t.Errorf("Test case %d failed: expected error %v but got %v", i+1, testCase.expectedErr, err)
			continue
		}

		if assignedNode != nil && (assignedNode.key != testCase.expectedNode.key || assignedNode.value != testCase.expectedNode.value) {
			t.Errorf("Test case %d failed: expected node %+v but got %+v", i+1, *testCase.expectedNode, *assignedNode)
		}
	}
}