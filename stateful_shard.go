package shuffle

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/getlantern/deepcopy"
	"github.com/mxschmitt/golang-combinations"
)

type StatefulSharder struct {
	store map[string]bool
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewStatefulSharder() *StatefulSharder {
	sharder := &StatefulSharder{}
	sharder.store = map[string]bool{}
	return sharder
}

func (shard *StatefulSharder) StatefulShuffleShard(lattice *Lattice, endpointsPerCell, maximumOverlap int) (*Lattice, error) {
	targetLattice, err := shard.shuffleShardRecursiveHelper(lattice, endpointsPerCell, maximumOverlap)
	if err != nil {
		return nil, err
	}

	if len(targetLattice.GetAllEndpoints()) == 0 {
		return nil, fmt.Errorf("No shards available")
	}

	for _, fragment := range combinations.Combinations(targetLattice.GetAllEndpoints(), maximumOverlap+1) {
		shard.saveFragment(fragment)
	}
	return targetLattice, nil
}

func (shard *StatefulSharder) shuffleShardRecursiveHelper(lattice *Lattice, endpointsPerCell, maximumOverlap int) (*Lattice, error) {
	allCoordinates := lattice.GetAllCoordinates()

	rand.Shuffle(len(allCoordinates), func(i, j int) {
		allCoordinates[i], allCoordinates[j] = allCoordinates[j], allCoordinates[i]
	})
	for _, coordinate := range allCoordinates {
		compliment, err := NewLattice(lattice.GetDimensionNames())
		if err != nil {
			return nil, err
		}
		err = deepcopy.Copy(compliment, lattice)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(lattice.GetDimensionality()); i++ {
			compliment, err = compliment.SimulateFailure(lattice.GetDimensionName(i), coordinate[i])
			if err != nil {
				return nil, err
			}
		}

		endpoints, err := lattice.GetEndpointsForSector(coordinate)
		if err != nil {
			return nil, err
		}
		rand.Shuffle(len(endpoints), func(i, j int) {
			endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
		})
		for _, fragment := range combinations.Combinations(endpoints, endpointsPerCell) {
			if len(fragment) >= maximumOverlap && shard.areThereTooManyCollisions(fragment, maximumOverlap) {
				continue
			}

			pickedRecursively, err := shard.shuffleShardRecursiveHelper(compliment, endpointsPerCell, maximumOverlap)
			if err != nil {
				return nil, err
			}
			combined := append(pickedRecursively.GetAllEndpoints(), fragment...)

			if len(combined) >= maximumOverlap && shard.areThereTooManyCollisions(combined, maximumOverlap) {
				continue
			}

			pickedRecursively.AddEndpointsForSector(coordinate, fragment)

			return pickedRecursively, nil
		}
	}

	return NewLattice(lattice.GetDimensionNames())
}

func (shard *StatefulSharder) saveFragment(fragment []string) {
	sort.Strings(fragment)
	shard.store[fmt.Sprintf("%v", fragment)] = true
}

func (shard *StatefulSharder) isFragmentUsed(fragment []string) bool {
	sort.Strings(fragment)
	_, ok := shard.store[fmt.Sprintf("%v", fragment)]
	return ok
}

func (shard *StatefulSharder) areThereTooManyCollisions(haystack []string, maximumOverlap int) bool {
	if len(haystack) <= maximumOverlap {
		return false
	} else if len(haystack) == maximumOverlap+1 {
		return shard.isFragmentUsed(haystack)
	}

	for _, fragment := range combinations.Combinations(haystack, maximumOverlap+1) {
		if shard.isFragmentUsed(fragment) {
			return true
		}
	}
	return false
}
