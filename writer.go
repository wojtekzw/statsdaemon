package main

type NullWriter struct{}

func (nw *NullWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}
