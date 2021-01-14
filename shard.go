// Package shuffle is a implementation of Amazon's Shuffle Sharding technique,
// a part of Route53's Infima library.
// This package implements the "simple signature" version of the sharding.
// Shards generated by this implementation are probabilistic and derived from
// a hash of identifiers.
//      Reference: https://github.com/awslabs/route53-infima.
package shuffle

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/spaolacci/murmur3"
)

// We need a seed value for the random stuff.
var seed = time.Now().UTC().UnixNano()

// SimpleShuffleShard implementation uses simple probabilistic hashing to
// compute shuffle shards. This function takes an existing lattice and
// generates a new sharded lattice for the given indentification and
// required number of endpoints with the sharded endpoints.
func (l *Lattice) SimpleShuffleShard(id []byte, epc int) (*Lattice, error) {
	var (
		r       *rand.Rand
		shdSeed int64

		shuffled [][]string
		eps      []string
		coords   []string
		minDim   int64
		dimVals  []string
		dimC     map[string]int
		shard    *Lattice

		err error
	)

	// Create a seed a random generator.
	shdSeed = int64(murmur3.Sum64WithSeed(id, uint32(l.Seed)))
	r = rand.New(rand.NewSource(l.Seed * shdSeed * 42))

	// The "chosen" lattice, which will have the sharded endpoints.
	shard, err = NewLatticeWithSeed(l.Seed, l.GetDimensionNames())
	if err != nil {
		return nil, fmt.Errorf(
			"shard: unable to create a sharded lattice: %v", err,
		)
	}

	// Shuffle the order of the values in each dimension.
	shuffled = [][]string{}
	for _, d := range l.GetDimensionNames() {
		dimVals = l.GetDimensionValues(d)
		r.Shuffle(len(dimVals), func(x, y int) {
			dimVals[x], dimVals[y] = dimVals[y], dimVals[x]
		})
		shuffled = append(shuffled, dimVals)
	}

	// Get the dimensionality of the lattice.
	dimC = l.GetDimensionality()

	// One dimensional lattices are a special case. For a one dimensional
	// lattice, we select end-points from each cell, since there is no other
	// dimension to consider.
	if len(dimC) == 1 {
		for _, dimVal := range shuffled[0] {
			eps, err = l.GetEndpointsForSector([]string{dimVal})
			if err != nil {
				return nil, err
			}

			r.Shuffle(len(eps), func(x, y int) {
				eps[x], eps[y] = eps[y], eps[x]
			})
			err = shard.AddEndpointsForSector([]string{dimVal}, eps[:epc])
			if err != nil {
				return nil, fmt.Errorf(
					"shard: unable to add endpoints: %v", err,
				)
			}
		}

		return shard, nil
	}

	// Otherwise, this is a multi-dimensional lattice.
	minDim = math.MaxInt64

	// Which dimension has the smallest number of values in it?
	for _, v := range dimC {
		if int64(v) < minDim {
			minDim = int64(v)
		}
	}

	// Build a coordinate to the chosen cells by picking the current top
	// item on each list of dimension values.
	for i := int64(0); i < minDim; i++ {
		coords = []string{}
		for j := 0; j < len(l.GetDimensionNames()); j++ {
			coords = append(coords, shuffled[j][0])
			shuffled[j] = shuffled[j][1:]
		}

		eps, err = l.GetEndpointsForSector(coords)
		if err != nil {
			return nil, fmt.Errorf("shard: unable to get endpoints: %v", err)
		} else if len(eps) <= 0 {
			return nil, fmt.Errorf("shard: no endpoints available")
		}

		r.Shuffle(len(eps), func(x, y int) {
			eps[x], eps[y] = eps[y], eps[x]
		})

		shard.AddEndpointsForSector(coords, eps[:epc])
	}

	return shard, nil
}
