//go:build race

package migrate

// raceDetectorEnabled lets a test skip itself under -race. See its use in the
// integration tests for why that is necessary here.
const raceDetectorEnabled = true
