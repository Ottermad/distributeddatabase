package partitions

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMapNodesToPartitions(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	nodesToPartition := MapNodesToPartitions(nodes)
	require.Len(t, nodesToPartition["node1"], 334)
	require.Len(t, nodesToPartition["node2"], 333)
	require.Len(t, nodesToPartition["node3"], 333)

	total := 0
	for _, partitions := range nodesToPartition {
		total += len(partitions)
	}

	require.Equal(t, total, numberOfPartitions)
}
