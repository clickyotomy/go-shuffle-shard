package shuffle_test

import (
    "fmt"

    "github.com/clickyotomy/go-shuffle-shard"
)

func ExampleNewLattice() {
    l, err := shuffle.NewLattice([]string{"az", "go-lang"})
    if err != nil {
        fmt.Printf("unable to create a lattice: %v", err)
    }

    fmt.Printf("%v\n", l.GetDimensionNames())

    // Output: [az go-lang]
}

func Example_AddEndpointsForSector() {
    l, err := shuffle.NewLattice([]string{"az", "go-lang"})
    if err != nil {
        fmt.Printf("unable to create a lattice: %v", err)
    }

    l.AddEndpointsForSector([]string{"us-east-1", "0.9"}, []string{"x", "y"})
    l.AddEndpointsForSector([]string{"us-west-1", "1.1"}, []string{"a", "b"})

    fmt.Printf("%v\n", l.GetDimensionNames())
    fmt.Printf("%v\n", l.GetAllEndpoints())

    e, err := l.GetEndpointsForSector([]string{"us-east-1", "0.9"})
    if err != nil {
        fmt.Printf(
            "unable to fetch endpoints for sector %v: %v",
            []string{"us-east-1", "0.9"}, err,
        )
    }
    fmt.Printf("%v\n", e)

    // Output:
    // [az go-lang]
    // [a b x y]
    // [x y]
}

func ExampleLattice_SimulateFailure() {
    l, err := shuffle.NewLattice([]string{"az", "go-lang"})
    if err != nil {
        fmt.Printf("unable to create a lattice: %v", err)
    }

    l.AddEndpointsForSector([]string{"us-east-1", "0.9"}, []string{"x", "y"})
    l.AddEndpointsForSector([]string{"us-west-1", "1.1"}, []string{"a", "b"})

    fmt.Printf("%v\n", l.GetAllEndpoints())

    s, err := l.SimulateFailure("az", "us-east-1")
    if err != nil {
        fmt.Printf("unable to simulate failure for lattice: %v", err)
    }
    fmt.Printf("%v\n", s.GetAllEndpoints())

    // Output:
    // [a b x y]
    // [a b]
}

func ExampleLattice_SimpleShuffleShard() {
    l, err := shuffle.NewLattice([]string{"az", "go-lang"})
    if err != nil {
        fmt.Printf("unable to create a lattice: %v", err)
    }

    l.AddEndpointsForSector([]string{"us-east-1", "0.9"}, []string{"x", "y"})
    l.AddEndpointsForSector([]string{"us-east-1", "1.1"}, []string{"a", "b"})
    l.AddEndpointsForSector([]string{"us-west-1", "0.9"}, []string{"c", "d"})
    l.AddEndpointsForSector([]string{"us-west-1", "1.1"}, []string{"e", "f"})

    s, err := l.SimpleShuffleShard([]byte{42}, 1)
    if err != nil {
        fmt.Printf("unable to shard the lattice: %v", err)
    }

    // Should return a pair of endpoints.
    fmt.Printf("%v\n", s.GetAllEndpoints())
}
