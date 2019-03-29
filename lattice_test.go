package shuffle_test

import (
	"strings"
	"testing"

	"github.com/clickyotomy/go-shuffle-shard"
)

// TestNewLattice tests the creation of a new lattice.
func TestNewLattice(t *testing.T) {
	var (
		s, d *shuffle.Lattice
		err  error
	)

	// Test for a single cell lattice.
	s, err = shuffle.NewLattice([]string{"dimX"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v", err)
	}

	s.AddEndpointsForSector([]string{"x"}, []string{"foo"})
	s.AddEndpointsForSector([]string{"x"}, []string{"bar", "baz"})

	if strings.Join(s.GetAllEndpoints(), ", ") != "bar, baz, foo" {
		t.Fatalf(
			`illegal endpoints returned: expected: "bar, baz, foo", `+
				`but got: "%s"`, strings.Join(s.GetAllEndpoints(), ", "),
		)
	}

	// Test for multi-dimensional lattice.
	d, err = shuffle.NewLattice([]string{"az", "go-lang"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v", err)
	}

	d.AddEndpointsForSector([]string{"us-x", "1.10"}, []string{"foo", "bar"})
	d.AddEndpointsForSector([]string{"us-y", "1.12"}, []string{"baz", "qux"})

	if strings.Join(d.GetAllEndpoints(), ", ") != "bar, baz, foo, qux" {
		t.Fatalf(
			`illegal endpoints returned: expected: "bar, baz, foo, qux", `+
				`but got: "%s"`, strings.Join(d.GetAllEndpoints(), ", "),
		)
	}
}

// TestAddEndpointsForSector checks if the returned endpoints are in order.
func TestAddEndpointsForSector(t *testing.T) {
	// Test for repeated endpoints.
	l, err := shuffle.NewLattice([]string{"az", "go-lang"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v", err)
	}

	err = l.AddEndpointsForSector(
		[]string{"us-x", "1.1"}, []string{"foo", "foo"},
	)
	if err != nil {
		t.Fatalf("unable to add endpoints to sector: %v", err)
	}

	err = l.AddEndpointsForSector(
		[]string{"us-y", "0.3"}, []string{"bar", "baz"},
	)
	if err != nil {
		t.Fatalf("unable to add endpoints to sector: %v", err)
	}

	if strings.Join(l.GetAllEndpoints(), ", ") != "bar, baz, foo" {
		t.Fatalf(
			`illegal endpoints returned: expected: "bar, baz, foo", `+
				`but got: "%s"`, strings.Join(l.GetAllEndpoints(), ", "),
		)
	}

	// Check for mismatch between dimensions and sector.
	err = l.AddEndpointsForSector([]string{"us-z"}, []string{"qux"})
	if err == nil {
		t.Fatalf("expected error for mismatched dimensions, but got %v", err)
	}
}

// TestGetEndpointsForSector checks if the right endpoints are returned
// for a sector.
func TestGetEndpointsForSector(t *testing.T) {
	var (
		l   *shuffle.Lattice
		s   []string
		e   []string
		err error
	)

	l, err = shuffle.NewLattice([]string{"az", "go-lang"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	l.AddEndpointsForSector([]string{"us-x", "1.1"}, []string{"foo", "foo"})
	l.AddEndpointsForSector([]string{"us-x", "0.3"}, []string{"qux", "foo"})
	l.AddEndpointsForSector([]string{"us-y", "0.3"}, []string{"bar", "baz"})
	l.AddEndpointsForSector([]string{"us-y", "1.1"}, []string{"xyzzy"})

	s = []string{"us-x", "0.3"}
	e, err = l.GetEndpointsForSector(s)
	if err != nil {
		t.Fatalf("unable to fetch endpoints for %s: %v", s, err)
	}

	if strings.Join(e, ", ") != "foo, qux" {
		t.Fatalf(
			`illegal endpoints returned: expected: "foo, qux", `+
				`but got: "%s"`, strings.Join(e, ", "),
		)
	}
}

// TestGetAllCoordinates checks if all the sector coordinates are returned.
func TestGetAllCoordinates(t *testing.T) {
	var (
		l   *shuffle.Lattice
		c   [][]string
		s   []string
		q   string
		err error
	)

	l, err = shuffle.NewLattice([]string{"az", "go-lang"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	l.AddEndpointsForSector([]string{"us-x", "1.1"}, []string{"foo", "foo"})
	l.AddEndpointsForSector([]string{"us-x", "0.3"}, []string{"qux", "foo"})
	l.AddEndpointsForSector([]string{"us-y", "1.1"}, []string{"xyzzy"})
	l.AddEndpointsForSector([]string{"us-y", "0.3"}, []string{"bar", "baz"})

	q = "[us-x, 0.3], [us-x, 1.1], [us-y, 0.3], [us-y, 1.1]"

	c = l.GetAllCoordinates()

	for _, i := range c {
		s = append(s, "["+strings.Join(i, ", ")+"]")
	}

	if strings.Join(s, ", ") != q {
		t.Fatalf(
			"illegal coordinates returned: expected %s, but got %s",
			strings.Join(s, ", "), q,
		)
	}
}

// TestGetDimensionality checks if the right dimensionality is returned.
func TestGetDimensionality(t *testing.T) {
	l, err := shuffle.NewLattice([]string{"az", "go-lang", "nginx"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	l.AddEndpointsForSector(
		[]string{"us-x", "1.1", "3"}, []string{"foo", "bar", "baz"},
	)
	l.AddEndpointsForSector(
		[]string{"us-x", "0.3", "3"}, []string{"qux", "xyzzy"},
	)
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	for k, v := range l.GetDimensionality() {
		if (k == "az" || k == "nginx") && v != 1 {
			t.Fatalf(
				"invalid cardinality returned (%s): expected: %d, but got: %d",
				k, 1, v,
			)
		} else if k == "go-lang" && v != 2 {
			t.Fatalf(
				"invalid cardinality returned (%s): expected: %d, but got: %d",
				k, 2, v,
			)
		}
	}
}

// TestGetDimensionNames tests if the right dimensions are returned.
func TestGetDimensionNames(t *testing.T) {
	l, err := shuffle.NewLattice([]string{"az", "go-lang", "nginx"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	if strings.Join(l.GetDimensionNames(), ", ") != "az, go-lang, nginx" {
		t.Fatalf(
			`invalid dimensions returned: expected: "az, go-lang, nginx" `+
				`but got %s`, strings.Join(l.GetDimensionNames(), ", "),
		)
	}
}

// TestGetDimensionNames tests if the right dimension is returned.
func TestGetDimensionName(t *testing.T) {
	l, err := shuffle.NewLattice([]string{"az", "go-lang", "nginx"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	if l.GetDimensionName(1) != "go-lang" {
		t.Fatalf(
			`invalid dimensions returned: expected: "go-lang" `+
				`but got %s`, l.GetDimensionName(1),
		)
	}
}

// TestGetDimensionValues checks if the right values for a dimension
// are returned.
func TestGetDimensionValues(t *testing.T) {
	l, err := shuffle.NewLattice([]string{"az", "go-lang", "nginx"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	l.AddEndpointsForSector(
		[]string{"us-x", "1.1", "3"}, []string{"foo", "bar", "baz"},
	)
	l.AddEndpointsForSector(
		[]string{"us-y", "0.3", "3"}, []string{"qux", "xyzzy"},
	)
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	if strings.Join(l.GetDimensionValues("az"), ", ") != "us-x, us-y" {
		t.Fatalf(
			`invalid dimensions returned: expected: "us-x, us-y" `+
				`but got %s`, strings.Join(l.GetDimensionValues("az"), ", "),
		)
	}
}

// TestGetDimensionSize tests if the right number of coordinates are returned.
func TestGetDimensionSize(t *testing.T) {
	l, err := shuffle.NewLattice([]string{"az", "go-lang", "nginx"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	if l.GetDimensionSize("az") != 0 {
		t.Fatalf(
			`invalid dimensions returned: expected: 0 `+
				`but got %d`, l.GetDimensionSize("az"),
		)
	}
}

// TestSimulateFailure does failure simulations. Cool stuff.
func TestSimulateFailure(t *testing.T) {
	l, err := shuffle.NewLattice([]string{"az", "go-lang"})
	if err != nil {
		t.Fatalf("unable to create a lattice: %v\n", err)
	}

	ep := [][]string{
		[]string{"a", "b", "c", "d", "e"},
		[]string{"f", "g", "h", "i", "j"},
		[]string{"k", "l", "m", "n", "o"},
		[]string{"p", "q", "r", "s", "t"},
	}

	l.AddEndpointsForSector([]string{"us-x", "1.1"}, ep[0])
	l.AddEndpointsForSector([]string{"us-x", "0.3"}, ep[1])
	l.AddEndpointsForSector([]string{"us-y", "1.1"}, ep[2])
	l.AddEndpointsForSector([]string{"us-y", "0.3"}, ep[3])

	if len(l.GetAllEndpoints()) != 20 {
		t.Fatalf(
			"wrong number of endpoints returned: expected: 20, but got: %d",
			len(l.GetAllEndpoints()),
		)
	}

	var s *shuffle.Lattice

	s, err = l.SimulateFailure("az", "us-x")
	if err != nil {
		t.Fatalf("unable to simulate a failure: %v", err)
	}
	if len(s.GetAllEndpoints()) != 10 {
		t.Fatalf(
			"wrong number of endpoints returned: expected: 10, but got: %d",
			len(s.GetAllEndpoints()),
		)
	}

	s, err = l.SimulateFailure("az", "us-y")
	if err != nil {
		t.Fatalf("unable to simulate a failure: %v", err)
	}
	if len(s.GetAllEndpoints()) != 10 {
		t.Fatalf(
			"wrong number of endpoints returned: expected: 10, but got: %d",
			len(s.GetAllEndpoints()),
		)
	}

	s, err = s.SimulateFailure("go-lang", "1.1")
	if err != nil {
		t.Fatalf("unable to simulate a failure: %v", err)
	}
	if len(s.GetAllEndpoints()) != 5 {
		t.Fatalf(
			"wrong number of endpoints returned: expected: 5, but got: %d",
			len(s.GetAllEndpoints()),
		)
	}

	s, err = s.SimulateFailure("go-lang", "0.3")
	if err != nil {
		t.Fatalf("unable to simulate a failure: %v", err)
	}
	if len(s.GetAllEndpoints()) != 0 {
		t.Fatalf(
			"wrong number of endpoints returned: expected: 0, but got: %d",
			len(s.GetAllEndpoints()),
		)
	}
}
