// Copyright (c) 2021  Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package net contains network-related helpers for integration testing.
package net

import (
	"net"
	"strconv"
)

// GetAvailablePort retrieves an available port on the host machine. This
// delegates the port selection to the golang net library by starting a server and
// then checking the port that the server is using.
func GetAvailablePort() (int, error) {
	l, err := net.Listen("tcp", ":0") // nolint: gosec
	if err != nil {
		return 0, nil
	}
	defer l.Close() // nolint: errcheck

	_, p, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		return 0, err
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		return 0, err
	}
	return port, nil
}

// MaybeGeneratePort takes an address and generates a new version of
// that address with the port replaced with an open port if the original
// port was 0. Otherwise, the address is returned unchanged.
// This method returns the potentially updated address, the potentially
// updated port, a boolean indicating if the address was changed, and
// an error in case the method failed
func MaybeGeneratePort(address string) (string, int, bool, error) {
	_, p, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, false, err
	}

	port, err := strconv.Atoi(p)
	if err != nil {
		return "", 0, false, err
	}

	if port == 0 {
		if address, port, err = GeneratePort(address); err != nil {
			return "", 0, false, err
		}
	}

	return address, port, false, nil
}

// GeneratePort takes the address in the string provided and updates
// the port to one that is open. Returns the complete address, the
// chosen port, and the error, if any.
func GeneratePort(address string) (string, int, error) {
	newPort, err := GetAvailablePort()
	if err != nil {
		return "", 0, err
	}

	h, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}

	newAddr := net.JoinHostPort(h, strconv.Itoa(newPort))

	return newAddr, newPort, nil
}
