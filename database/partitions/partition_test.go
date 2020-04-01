package partitions

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMapNodesToPartitions(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	partitionsToNodes := MapPartitionsToNodes(nodes)

	nodesToPartition := map[string]int {
		"node1": 0,
		"node2": 0,
		"node3": 0,
	}
	for _, partition := range partitionsToNodes {
		nodesToPartition[partition.Node] += 1
	}

	require.Equal(t, nodesToPartition["node1"], 334)
	require.Equal(t, nodesToPartition["node2"], 333)
	require.Equal(t, nodesToPartition["node3"], 333)

	require.Equal(t, len(partitionsToNodes), numberOfPartitions)
}
