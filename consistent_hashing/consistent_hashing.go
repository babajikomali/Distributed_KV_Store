package consistent_hashing

import (
	"errors"
	"hash/crc32"
)

type node struct {
	key   uint32
	value string
}

type hashRing struct {
	nodes             []node
	maxNodes          int
	replicationFactor int
}

func InitHashRing(maxNodes int, replicationFactor int) *hashRing {
	return &hashRing{
		nodes:             []node{},
		maxNodes:          maxNodes,
		replicationFactor: replicationFactor,
	}
}

func (hr *hashRing) hashFunc(hashInput string) (uint32, error) {
	checksumTable := crc32.MakeTable(crc32.Castagnoli)
	hashOutput := crc32.Checksum([]byte(hashInput), checksumTable)
	return uint32(hashOutput), nil
}

func (hr *hashRing) binarySearchAdd(val uint32) int {
	low := 0
	high := len(hr.nodes) - 1
	result := -1

	for low <= high {
		mid := low + (high-low)/2
		if hr.nodes[mid].key > val {
			high = mid - 1
		} else {
			result = mid
			low = mid + 1
		}
	}

	return result
}

func (hr *hashRing) binarySearchRemove(val uint32) int {
	low := 0
	high := len(hr.nodes) - 1

	for low <= high {
		mid := low + (high-low)/2
		if hr.nodes[mid].key > val {
			high = mid - 1
		} else if hr.nodes[mid].key < val {
			low = mid + 1
		} else {
			return mid
		}
	}

	return -1
}

func (hr *hashRing) binarySearchAssign(val uint32) int {
	low := 0
	high := len(hr.nodes) - 1
	result := 0

	for low <= high {
		mid := low + (high-low)/2
		if hr.nodes[mid].key < val {
			low = mid + 1
		} else {
			result = mid
			high = mid - 1
		}
	}

	return result
}

func (hr *hashRing) addNode(value string) (*node, error) {
	if len(hr.nodes) == hr.maxNodes {
		return &node{}, errors.New("no slot available for node in hash ring")
	}

	hashOutput, err := hr.hashFunc(value)
	if err != nil {
		return &node{}, err
	}

	index := hr.binarySearchAdd(hashOutput)
	if index != -1 && hr.nodes[index].key == hashOutput {
		return &node{}, errors.New("hash collision occured with the node")
	}
	index = index + 1
	hr.nodes = append(hr.nodes, node{value: "", key: 0})
	copy(hr.nodes[index+1:], hr.nodes[index:])
	hr.nodes[index] = node{key: hashOutput, value: value}
	return &hr.nodes[index], nil
}

func (hr *hashRing) removeNode(value string) (*node, error) {
	if len(hr.nodes) == 0 {
		return &node{}, errors.New("empty hash ring")
	}

	hashOutput, err := hr.hashFunc(value)
	if err != nil {
		return &node{}, err
	}
	index := hr.binarySearchRemove(hashOutput)
	if index == -1 {
		return &node{}, errors.New("node doesn't exist")
	}

	hr.nodes = append(hr.nodes[:index], hr.nodes[index+1:]...)

	return &hr.nodes[index], nil
}

func (hr *hashRing) assignNode(value string) (*node, error) {
	hashOutput, err := hr.hashFunc(value)
	if err != nil {
		return nil, err
	}
	index := hr.binarySearchAssign(hashOutput)
	return &hr.nodes[index], nil
}
