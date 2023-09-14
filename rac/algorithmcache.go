package rac

// A small in-memory store to store a few algorithms, such that they do not have to be requeried from the ingress db everytime.

type AlgorithmCache struct {
	// Key is a byte array, but cast to a string. This takes advantage of the rules for string types and conversions in the Go Spec;
	// Strings behave like arrays of bytes but are immutable
	Algorithms map[string][]byte
}
