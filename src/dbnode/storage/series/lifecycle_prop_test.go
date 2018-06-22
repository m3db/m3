package series

// This file uses a SUT prop test to ensure we correctly manage the lifecycle of a series, including
// its mutable buffer, immutable blocks, and all of the interactions between them.

// func TestSeriesLifecycle(t *testing.T) {
// 	parameters := gopter.DefaultTestParameters()
// 	seed := time.Now().UnixNano()
// 	parameters.MinSuccessfulTests = 100
// 	parameters.MaxSize = 40
// 	parameters.Rng = rand.New(rand.NewSource(seed))
// 	properties := gopter.NewProperties(parameters)
// 	// commds
// 	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
// 	if !properties.Run(reporter) {
// 		t.Errorf("failed with initial seed: %d", seed)
// 	}
// }

// func TestSeriesMerge
