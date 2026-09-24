package backpressure

import (
	"strconv"
	"testing"
	"testing/synctest"
)

func TestHealthyStartupOpensBoundedWindow(t *testing.T) {
	for _, maximum := range []int{1, 4, 64} {
		t.Run(strconv.Itoa(maximum), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				opts := Options{Store: "primary", Role: "engine", MaxConcurrent: maximum}
				controller, err := New(opts)
				if err != nil {
					t.Fatal(err)
				}
				if err := controller.Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
				if want := min(4, maximum); controller.limit != want {
					t.Fatalf("healthy initial window = %d, want %d", controller.limit, want)
				}
			})
		})
	}
}
