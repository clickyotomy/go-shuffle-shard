package shuffle_test

import (
	"math"
	"testing"

	"github.com/clickyotomy/go-shuffle-shard"
)

// The maxium variation allowed (maximum: 40%).
const distributionThreshold float64 = 0.4

// almost is a helper function for asserting floats, within a threshold.
func almost(a, b, t float64) bool {
	return math.Abs(a-b) <= t
}

// contains checks if an element is present in a slice.
func contains(e string, s []string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// TestSimpleShuffleShard tests the generation of sharded lattices.
func TestSimpleShuffleShard(t *testing.T) {
	var (
		lat *shuffle.Lattice
		shd *shuffle.Lattice
		frq map[string]int
		eps []string
		err error
		ep  string
		ok  bool
	)

	// Initialize.
	eps = []string{}
	frq = map[string]int{}

	for i := 97; i < 117; i++ {
		eps = append(eps, string(i))
	}

	// For a single cell lattice (with 20 endpoints).
	lat, err = shuffle.NewLattice([]string{"dimX"})
	if err != nil {
		t.Fatalf("unable to create a new lattice: %v", err)
	}
	lat.AddEndpointsForSector([]string{"x"}, eps)

	for i := 0; i < 100000; i++ {
		shd, err = lat.SimpleShuffleShard([]byte{byte(i)}, 4)
		if err != nil {
			t.Fatalf("unable to shard the lattice: %v", err)
		}

		if len(shd.GetAllEndpoints()) != 4 {
			t.Fatalf(
				"illegal number of endpoints returned: expected 4, "+
					"but got: %d", len(shd.GetAllEndpoints()),
			)
		}

		if len(shd.GetDimensionality()) != 1 {
			t.Fatalf(
				"illegal number of dimensions returned: expected 1, "+
					"but got: %d", len(shd.GetDimensionality()),
			)
		}

		if len(shd.GetAllCoordinates()) != 1 {
			t.Fatalf(
				"illegal number of coordinates returned: expected 1, "+
					"but got: %d", len(shd.GetAllCoordinates()),
			)
		}

		for _, ep = range shd.GetAllEndpoints() {
			if _, ok = frq[ep]; ok {
				frq[ep]++
			} else {
				frq[ep] = 0
			}
		}
	}

	// Check that all 20 letters were seen.
	if len(frq) != 20 {
		t.Fatalf(
			"bad sharding, uneven endpoint distribution: expected 20, "+
				"but got %d\ndistribution: %v", len(frq), frq,
		)
	}

	// We computed 100,000 shards with 4 endpoints each. There are a total of
	// 20 endpoints, so each is expected to be seen 400,000 / 20 == 20,000.
	// Check that we're within maximum threshold percentage for each letter.
	for k, v := range frq {
		if !almost(float64(v)/20000, float64(1.0), distributionThreshold) {
			t.Fatalf(
				"the endpoint distribution exceeds the threshold {%s: %d}: "+
					"expected: %f, but got: %f",
				k, v, float64(1.0), float64(v)/20000,
			)
		}
	}

	// For a 1-D lattice (with 20 endpoints).
	frq = map[string]int{}
	lat, err = shuffle.NewLattice([]string{"az"})
	if err != nil {
		t.Fatalf("unable to create a new lattice: %v", err)
	}
	lat.AddEndpointsForSector([]string{"us-x"}, eps[:len(eps)/2])
	lat.AddEndpointsForSector([]string{"us-y"}, eps[len(eps)/2:])

	for i := 0; i < 100000; i++ {
		shd, err = lat.SimpleShuffleShard([]byte{byte(i)}, 2)
		if err != nil {
			t.Fatalf("unable to shard the lattice: %v", err)
		}

		if len(shd.GetAllEndpoints()) != 4 {
			t.Fatalf(
				"illegal number of endpoints returned: expected 4, "+
					"but got: %d", len(shd.GetAllEndpoints()),
			)
		}

		if len(shd.GetDimensionality()) != 1 {
			t.Fatalf(
				"illegal number of dimensions returned: expected 1, "+
					"but got: %d", len(shd.GetDimensionality()),
			)
		}

		if len(shd.GetAllCoordinates()) != 2 {
			t.Fatalf(
				"illegal number of coordinates returned: expected 1, "+
					"but got: %d", len(shd.GetAllCoordinates()),
			)
		}

		for _, ep = range shd.GetAllEndpoints() {
			if _, ok = frq[ep]; ok {
				frq[ep]++
			} else {
				frq[ep] = 0
			}
		}
	}

	// Check that all 20 letters were seen.
	if len(frq) != 20 {
		t.Fatalf(
			"bad sharding, uneven endpoint distribution: expected 20, "+
				"but got %d\ndistribution: %v", len(frq), frq,
		)
	}

	// We computed 100,000 shards with 4 endpoints each. There are a total of
	// 20 endpoints, so each is expected to be seen 400,000 / 20 == 20,000.
	// Check that we're within maximum threshold percentage for each letter.
	for k, v := range frq {
		if !almost(float64(v)/20000, float64(1.0), distributionThreshold) {
			t.Fatalf(
				"the endpoint distribution exceeds the threshold {%s: %d}: "+
					"expected: %f, but got: %f",
				k, v, float64(1.0), float64(v)/20000,
			)
		}
	}

	// For a 2-D lattice (with 20 endpoints).
	frq = map[string]int{}
	lat, err = shuffle.NewLattice([]string{"az", "version"})
	if err != nil {
		t.Fatalf("unable to create a new lattice: %v", err)
	}
	lat.AddEndpointsForSector([]string{"x", "1"}, eps[:len(eps)/4])
	lat.AddEndpointsForSector([]string{"y", "1"}, eps[len(eps)/4:len(eps)/2])
	lat.AddEndpointsForSector([]string{"x", "2"}, eps[len(eps)/2:3*len(eps)/4])
	lat.AddEndpointsForSector([]string{"y", "2"}, eps[3*len(eps)/4:])

	for i := 0; i < 100000; i++ {
		shd, err = lat.SimpleShuffleShard([]byte{byte(i)}, 2)

		if err != nil {
			t.Fatalf("unable to shard the lattice: %v", err)
		}

		if len(shd.GetAllEndpoints()) != 4 {
			t.Fatalf(
				"illegal number of endpoints returned: expected 4, "+
					"but got: %d", len(shd.GetAllEndpoints()),
			)
		}

		if len(shd.GetDimensionality()) != 2 {
			t.Fatalf(
				"illegal number of dimensions returned: expected 1, "+
					"but got: %d", len(shd.GetDimensionality()),
			)
		}

		if len(shd.GetAllCoordinates()) != 2 {
			t.Fatalf(
				"illegal number of coordinates returned: expected 2, "+
					"but got: %d", len(shd.GetAllCoordinates()),
			)
		}

		for _, ep = range shd.GetAllEndpoints() {
			if _, ok = frq[ep]; ok {
				frq[ep]++
			} else {
				frq[ep] = 0
			}
		}
	}

	// Confirm that endpoints stay in their own cells.
	epShd, err := shd.GetEndpointsForSector([]string{"x", "1"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep = range epShd {
		if !contains(ep, eps[:len(eps)/4]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[:len(eps)/4], ep,
			)
		}
	}

	epShd, err = shd.GetEndpointsForSector([]string{"y", "1"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep = range epShd {
		if !contains(ep, eps[len(eps)/4:len(eps)/2]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[len(eps)/4:len(eps)/2], ep,
			)
		}
	}

	epShd, err = shd.GetEndpointsForSector([]string{"x", "2"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep := range epShd {
		if !contains(ep, eps[len(eps)/2:3*len(eps)/4]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[len(eps)/2:3*len(eps)/4], ep,
			)
		}
	}

	epShd, err = shd.GetEndpointsForSector([]string{"y", "2"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep = range epShd {
		if !contains(ep, eps[3*len(eps)/4:]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[:len(eps)/4], ep,
			)
		}
	}

	// Check that all 20 letters were seen.
	if len(frq) != 20 {
		t.Fatalf(
			"bad sharding, uneven endpoint distribution: expected 20, "+
				"but got %d\ndistribution: %v", len(frq), frq,
		)
	}

	// We computed 100,000 shards with 4 endpoints each. There are a total of
	// 20 endpoints, so each is expected to be seen 400,000 / 20 == 20,000.
	// Check that we're within maximum threshold percentage for each letter.
	for k, v := range frq {
		if !almost(float64(v)/20000, float64(1.0), distributionThreshold) {
			t.Fatalf(
				"the endpoint distribution exceeds the threshold {%s: %d}: "+
					"expected: %f, but got: %f",
				k, v, float64(1.0), float64(v)/20000,
			)
		}
	}

	// For an asyemmetric 2-D lattice.
	// Generate a new set of endpoints.
	eps = []string{}
	frq = map[string]int{}

	for i := 97; i < 121; i++ {
		eps = append(eps, string(i))
	}

	lat, err = shuffle.NewLattice([]string{"az", "version"})
	if err != nil {
		t.Fatalf("unable to create a new lattice: %v", err)
	}

	lat.AddEndpointsForSector([]string{"x", "1"}, eps[:len(eps)/6])
	lat.AddEndpointsForSector([]string{"x", "2"}, eps[len(eps)/6:len(eps)/3])
	lat.AddEndpointsForSector([]string{"x", "3"}, eps[len(eps)/3:len(eps)/2])
	lat.AddEndpointsForSector([]string{"y", "1"}, eps[len(eps)/2:2*len(eps)/3])
	lat.AddEndpointsForSector(
		[]string{"y", "2"}, eps[2*len(eps)/3:5*len(eps)/6],
	)
	lat.AddEndpointsForSector([]string{"y", "3"}, eps[5*len(eps)/6:])

	for i := 0; i < 100000; i++ {
		shd, err = lat.SimpleShuffleShard([]byte{byte(i)}, 2)

		if err != nil {
			t.Fatalf("unable to shard the lattice: %v", err)
		}

		if len(shd.GetAllEndpoints()) != 4 {
			t.Fatalf(
				"illegal number of endpoints returned: expected 4, "+
					"but got: %d", len(shd.GetAllEndpoints()),
			)
		}

		if len(shd.GetDimensionality()) != 2 {
			t.Fatalf(
				"illegal number of dimensions returned: expected 1, "+
					"but got: %d", len(shd.GetDimensionality()),
			)
		}

		if len(shd.GetAllCoordinates()) != 2 {
			t.Fatalf(
				"illegal number of coordinates returned: expected 2, "+
					"but got: %d", len(shd.GetAllCoordinates()),
			)
		}

		for _, ep = range shd.GetAllEndpoints() {
			if _, ok = frq[ep]; ok {
				frq[ep]++
			} else {
				frq[ep] = 0
			}
		}
	}

	// Confirm that endpoints stay in their own cells.
	epShd, err = shd.GetEndpointsForSector([]string{"x", "1"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep = range epShd {
		if !contains(ep, eps[:len(eps)/6]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[:len(eps)/6], ep,
			)
		}
	}

	epShd, err = shd.GetEndpointsForSector([]string{"x", "2"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep = range epShd {
		if !contains(ep, eps[len(eps)/6:len(eps)/3]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[len(eps)/6:len(eps)/3], ep,
			)
		}
	}

	epShd, err = shd.GetEndpointsForSector([]string{"x", "3"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep := range epShd {
		if !contains(ep, eps[len(eps)/3:len(eps)/2]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[len(eps)/3:len(eps)/2], ep,
			)
		}
	}

	epShd, err = shd.GetEndpointsForSector([]string{"y", "1"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep = range epShd {
		if !contains(ep, eps[len(eps)/2:2*len(eps)/3]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[len(eps)/2:2*len(eps)/3], ep,
			)
		}
	}

	epShd, err = shd.GetEndpointsForSector([]string{"y", "2"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep = range epShd {
		if !contains(ep, eps[2*len(eps)/3:5*len(eps)/6]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[2*len(eps)/3:5*len(eps)/6], ep,
			)
		}
	}

	epShd, err = shd.GetEndpointsForSector([]string{"y", "3"})
	if err != nil {
		t.Fatalf("unable to fetch endpoints: %v", err)
	}

	for _, ep = range epShd {
		if !contains(ep, eps[5*len(eps)/6:]) {
			t.Fatalf(
				"bad endpoints returned: expected one of %v, but got: %s",
				eps[5*len(eps)/6:], ep,
			)
		}
	}

	// Check that all 24 letters were seen.
	if len(frq) != 24 {
		t.Fatalf(
			"bad sharding, uneven endpoint distribution: expected 20, "+
				"but got %d\ndistribution: %v", len(frq), frq,
		)
	}

	// We computed 100,000 shards with 4 endpoints each. There are a total of
	// 24 endpoints, so each is expected to be seen 400,000 / 24 ~= 16,666.
	// Check that we're within maximum threshold percentage for each letter.
	for k, v := range frq {
		if !almost(float64(v)/16666, float64(1.0), distributionThreshold) {
			t.Fatalf(
				"the endpoint distribution exceeds the threshold {%s: %d}: "+
					"expected: %f, but got: %f",
				k, v, float64(1.0), float64(v)/16666,
			)
		}
	}
}
