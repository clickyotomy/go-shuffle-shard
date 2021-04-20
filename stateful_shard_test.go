package shuffle_test

import (
	"testing"

	"github.com/clickyotomy/go-shuffle-shard"
)

func TestStatefulShuffleShardRunsOutOfShards(t *testing.T) {
	endpoints := []string{"A", "B", "C", "D", "E"}
	lattice, err := shuffle.NewLattice([]string{"dimX"})
	if err != nil {
		t.Fatalf("unable to create new lattice: %v", err)
	}
	lattice.AddEndpointsForSector([]string{"x"}, endpoints)

	sharder := shuffle.NewStatefulSharder()

	for i := 0; i < 2; i++ {
		_, err := sharder.StatefulShuffleShard(lattice, 4, 2)
		if err != nil && i != 1 {
			t.Fatalf("Should only have one valid shard from this config")
		} else if err != nil && err.Error() != "No shards available" {
			t.Fatalf("Unexpected error from StatefulShuffleShard failure")
		}
	}
}

func TestStatefulShuffleShardSingleCell(t *testing.T) {
	endpoints := []string{
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
		"K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
	}

	lattice, err := shuffle.NewLattice([]string{"dimX"})
	if err != nil {
		t.Fatalf("unable to create new lattice: %v", err)
	}
	lattice.AddEndpointsForSector([]string{"x"}, endpoints)

	sharder := shuffle.NewStatefulSharder()
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		shard, err := sharder.StatefulShuffleShard(lattice, 4, 2)
		if err != nil {
			t.Fatalf("Ran out of available shard combinations prematurely")
		}
		if len(shard.GetAllEndpoints()) != 4 {
			t.Fatalf("StatefulShuffleShard should return a lattice with an endpoint count matching the endpointPerCell value")
		}
		if len(shard.GetDimensionality()) != 1 {
			t.Fatalf("StatefulShuffleShard should return a lattice with the same dimensionality of the first")
		}
		if len(shard.GetAllCoordinates()) != 1 {
			t.Fatalf("StatefulShuffleShard should return a lattice with with the same input coordinates of the first")
		}

		for _, letter := range shard.GetAllEndpoints() {
			seen[letter] = true
		}
	}

	for _, letter := range endpoints {
		if _, ok := seen[letter]; !ok {
			t.Fatalf("Not all endpoints were seen after 100 stateful shuffles")
		}
	}
}

// 1-dimensional lattice with 20 endpoints
func TestStatefulShuffleShardOneDimensional(t *testing.T) {
	endpointsA := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	endpointsB := []string{"K", "L", "M", "N", "O", "P", "Q", "R", "S", "T"}

	lattice, err := shuffle.NewLattice([]string{"AZ"})
	if err != nil {
		t.Fatalf("unable tocreate new lattice: %v", err)
	}
	lattice.AddEndpointsForSector([]string{"az1"}, endpointsA)
	lattice.AddEndpointsForSector([]string{"az2"}, endpointsB)

	sharder := shuffle.NewStatefulSharder()

	seen := map[string]bool{}
	for i := 0; i < 45; i++ {
		shard, err := sharder.StatefulShuffleShard(lattice, 2, 2)
		if err != nil {
			t.Fatalf("Ran out of available shard combinations prematurely")
		}
		if len(shard.GetAllEndpoints()) != 4 {
			t.Fatalf("StatefulShuffleShard should return a lattice with an endpoint count matching the endpointPerCell * number of cells")
		}
		if len(shard.GetDimensionality()) != 1 {
			t.Fatalf("StatefulShuffleShard should return a lattice with the same dimensionality of the first")
		}
		if len(shard.GetAllCoordinates()) != 2 {
			t.Fatalf("StatefulShuffleShard should return a lattice with two cells")
		}

		letters, err := shard.GetEndpointsForSector([]string{"az1"})
		if err != nil {
			t.Fatalf("GetEndpointsForSector failed for az1 when it shouldn't have")
		}
		for _, letter := range letters {
			if !contains(letter, endpointsA) {
				t.Fatalf("az1 sector had an endpoint that didn't come from its list of endpoints")
			}
			seen[letter] = true
		}
		letters, err = shard.GetEndpointsForSector([]string{"az2"})
		if err != nil {
			t.Fatalf("GetEndpointsForSector failed for az2 when it shouldn't have")
		}
		for _, letter := range letters {
			if !contains(letter, endpointsB) {
				t.Fatalf("az2 sector had an endpoint that didn't come from its list of endpoints")
			}
			seen[letter] = true
		}
	}

	for _, letter := range append(endpointsA, endpointsB...) {
		if _, ok := seen[letter]; !ok {
			t.Fatalf("Not all endpoints were seen after 45 stateful shuffles")
		}
	}
}

func TestStatefulShuffleShardTwoDimensional(t *testing.T) {
	endpointsA1 := []string{"A", "B", "C", "D", "E"}
	endpointsA2 := []string{"F", "G", "H", "I", "J"}
	endpointsB1 := []string{"K", "L", "M", "N", "O"}
	endpointsB2 := []string{"P", "Q", "R", "S", "T"}

	lattice, err := shuffle.NewLattice([]string{"AZ", "Version"})
	if err != nil {
		t.Fatalf("unable tocreate new lattice: %v", err)
	}
	lattice.AddEndpointsForSector([]string{"az1", "1"}, endpointsA1)
	lattice.AddEndpointsForSector([]string{"az1", "2"}, endpointsA2)
	lattice.AddEndpointsForSector([]string{"az2", "1"}, endpointsB1)
	lattice.AddEndpointsForSector([]string{"az2", "2"}, endpointsB2)

	sharder := shuffle.NewStatefulSharder()

	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		shard, err := sharder.StatefulShuffleShard(lattice, 2, 2)
		if err != nil {
			t.Fatalf("Ran out of available shard combinations prematurely")
		}
		if len(shard.GetAllEndpoints()) != 4 {
			t.Fatalf("StatefulShuffleShard should return a lattice with an endpoint count matching the endpointPerCell * number of cells")
		}
		if len(shard.GetDimensionality()) != 2 {
			t.Fatalf("StatefulShuffleShard should return a lattice with the same dimensionality of the first")
		}
		if len(shard.GetAllCoordinates()) != 2 {
			t.Fatalf("StatefulShuffleShard should return a lattice with 2 coordinates")
		}

		letters, err := shard.GetEndpointsForSector([]string{"az1", "1"})
		if err != nil {
			t.Fatalf("GetEndpointsForSector failed for az1,1 when it shouldn't have")
		}
		for _, letter := range letters {
			if !contains(letter, endpointsA1) {
				t.Fatalf("az1,1 sector had an endpoint that didn't come from its list of endpoints")
			}
			seen[letter] = true
		}
		letters, err = shard.GetEndpointsForSector([]string{"az1", "2"})
		if err != nil {
			t.Fatalf("GetEndpointsForSector failed for az1,2 when it shouldn't have")
		}
		for _, letter := range letters {
			if !contains(letter, endpointsA2) {
				t.Fatalf("az1,2 sector had an endpoint that didn't come from its list of endpoints")
			}
			seen[letter] = true
		}
		letters, err = shard.GetEndpointsForSector([]string{"az2", "1"})
		if err != nil {
			t.Fatalf("GetEndpointsForSector failed for az2,1 when it shouldn't have")
		}
		for _, letter := range letters {
			if !contains(letter, endpointsB1) {
				t.Fatalf("az2,1 sector had an endpoint that didn't come from its list of endpoints")
			}
			seen[letter] = true
		}
		letters, err = shard.GetEndpointsForSector([]string{"az2", "2"})
		if err != nil {
			t.Fatalf("GetEndpointsForSector failed for az2,2 when it shouldn't have")
		}
		for _, letter := range letters {
			if !contains(letter, endpointsB2) {
				t.Fatalf("az2,2 sector had an endpoint that didn't come from its list of endpoints")
			}
			seen[letter] = true
		}
	}

	endpoints := append(endpointsA1, endpointsA2...)
	endpoints = append(endpoints, endpointsB1...)
	endpoints = append(endpoints, endpointsB2...)

	for _, letter := range endpoints {
		if _, ok := seen[letter]; !ok {
			t.Fatalf("Not all endpoints were seen after 20 stateful shuffles")
		}
	}
}
