package partitions

const numberOfPartitions = 1000

func MapNodesToPartitions(nodes []string) map[string][]int {
	numberOfNodes := len(nodes)
	partitionsPerNode := numberOfPartitions / numberOfNodes // Rounds down

	nodesToPartition := map[string][]int{}
	for _, node := range nodes {
		nodesToPartition[node] = []int{}
	}

	for partition := 1; partition <= numberOfPartitions; partition++ {
		// Cycle through nodes
		// If node not full add partition
		filled := false
		for _, node := range nodes {
			if (len(nodesToPartition[node])) >= partitionsPerNode {
				continue
			}

			nodesToPartition[node] = append(nodesToPartition[node], partition)
			filled = true
			break
		}

		if !filled {
			// If all nodes full then allow adding one extra partition
			for _, node := range nodes {
				if (len(nodesToPartition[node])) >= partitionsPerNode+1 {
					continue
				}

				nodesToPartition[node] = append(nodesToPartition[node], partition)
				break
			}
		}
	}

	return nodesToPartition
}
