package shuffle

import (
	"fmt"
	"sort"
	"strings"
)

type Lattice struct {
	// DimensionNames is a set of dimensions
	// that can be attributed to a lattice.
	// Note: We'll be using a map of string
	// to bool here instead of sets, because
	// go does not support it natively.
	DimensionNames []string

	// ValuesByDimension represents positions along the dimension
	// and form part of the sector coordinates for an end-point.
	// Each dimension has a valid set of values; for example, the
	// "AvailabilityZone" dimension may have the values:
	//      ["us-x", "us-y", ...].
	ValuesByDimension map[string][]string

	// EndpointsByCoordinate is a map of the end-points by the
	// sector coordinates. For example, if our dimensions are
	// "AvailabilityZone" and "SoftwareVersion" then we might
	// have something like:
	//      ["us-x", "v42"] -> [endpoints-in-us-x-running-v42].
	EndpointsByCoordinate map[string][]string
}

// We need this because we can't have a slice for a key in a map,
// which was intended to be used in `Lattice.EndpointsByCoordinate'.
// Also, a `⚡️' looks really cool!
const seperator = `⚡️`

// set is a helper function to convert a slice to a set (Python style).
func set(arr []string) []string {
	var (
		m = map[string]bool{}
		s = []string{}
	)

	for _, i := range arr {
		if _, ok := m[i]; !ok {
			m[i] = true
			s = append(s, i)
		}
	}

	sort.Strings(s)
	return s
}

// sliceIdx is a helper function to find the index of a matching string
// in a slice.
func indexOf(s []string, p string) int {
	for i, _ := range s {
		if s[i] == p {
			return i
		}
	}

	return -1
}

// NewLattice creates an N-dimensional Lattice for a given set of dimension
// names, where each dimension represents a meaningful availability axis.
func NewLattice(dims []string) (*Lattice, error) {
	if len(dims) == 0 {
		return nil, fmt.Errorf("lattice: at least one dimension is required")
	}

	// Initialize an empty lattice.
	l := &Lattice{[]string{}, map[string][]string{}, map[string][]string{}}

	// Sort the dimensions.
	sort.Strings(dims)

	// Add dimension names to the lattice.
	l.DimensionNames = set(dims)

	// Create an empty set for each of the dimensions.
	for _, d := range l.DimensionNames {
		l.ValuesByDimension[d] = []string{}
	}

	return l, nil
}

// AddEndpointsForSector adds all of the end-points for that are associated
// with a particular sector. The order of the sector should match the order
// of the dimensions the lattice was initialized with.
func (l *Lattice) AddEndpointsForSector(sec, ep []string) error {
	if len(sec) != len(l.DimensionNames) {
		return fmt.Errorf(
			"lattice: mismatch between dimensions of the lattice and sector",
		)
	}

	// Construct the key.
	k := strings.Join(sec, seperator)

	// If these are endpoints for that sector, append;
	// otherwise, create a new entry for that sector.
	e, ok := l.EndpointsByCoordinate[k]
	if ok {
		ep = append(ep, e...)
	}

	l.EndpointsByCoordinate[k] = set(ep)

	for i, d := range l.DimensionNames {
		l.ValuesByDimension[d] = append(l.ValuesByDimension[d], sec[i])
		l.ValuesByDimension[d] = set(l.ValuesByDimension[d])
	}

	return nil
}

// GetEndpointsForSector gets the endpoints in a particular sector.
func (l *Lattice) GetEndpointsForSector(sec []string) ([]string, error) {
	if len(sec) != len(l.DimensionNames) {
		return nil, fmt.Errorf(
			"lattice: mismatch between dimensions of the lattice and sector",
		)
	}

	return l.EndpointsByCoordinate[strings.Join(sec, seperator)], nil
}

// GetAllEndpoints gets all of the end-points in the lattice.
func (l *Lattice) GetAllEndpoints() []string {
	var e []string

	for _, v := range l.EndpointsByCoordinate {
		e = append(e, v...)
	}

	return set(e)
}

// GetAllCoordinates gets a list of all cells in the lattice.
func (l *Lattice) GetAllCoordinates() [][]string {
	var (
		t []string
		c [][]string
	)

	for k, _ := range l.EndpointsByCoordinate {
		t = append(t, k)
	}
	t = set(t)

	// Now, we have the set of keys, we should split them up into tuples.
	for _, k := range t {
		c = append(c, strings.Split(k, seperator))
	}

	return c
}

// GetDimensionality returns the number of dimensions a lattice has.
func (l *Lattice) GetDimensionality() map[string]int {
	m := make(map[string]int)

	for _, d := range l.DimensionNames {
		m[d] = len(l.GetDimensionValues(d))
	}

	return m
}

// GetDimensionNames gets the list of dimension names for a lattice.
func (l *Lattice) GetDimensionNames() []string {
	return l.DimensionNames
}

// GetDimensionName get the dimension name for a given numbered dimension.
func (l *Lattice) GetDimensionName(dNum int) string {
	return l.DimensionNames[dNum]
}

// GetDimensionValues gets the set of values for a given dimension.
func (l *Lattice) GetDimensionValues(dName string) []string {
	return set(l.ValuesByDimension[dName])
}

// GetDimensionSize returns the number of discrete
// coordinates are there in a given dimension.
func (l *Lattice) GetDimensionSize(dName string) int {
	return len(set(l.ValuesByDimension[dName]))
}

// SimulateFailure simulates failure of a
// particular slice of cells in the lattice.
func (l *Lattice) SimulateFailure(dName, dVal string) (*Lattice, error) {
	sublattice, err := NewLattice(l.DimensionNames)
	if err != nil {
		return nil, err
	}

	dIdx := indexOf(l.DimensionNames, dName)
	if dIdx < 0 {
		return nil, fmt.Errorf("lattice: unknown dimension name")
	}

	for c, _ := range l.EndpointsByCoordinate {
		// Because we joined as a key for `Lattice.EndpointsByCoordinate'.
		s := strings.Split(c, seperator)
		if s[dIdx] != dVal {
			k := strings.Join(s, seperator)
			sublattice.AddEndpointsForSector(s, l.EndpointsByCoordinate[k])
		}
	}

	return sublattice, nil
}
