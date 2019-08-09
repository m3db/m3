package foo

import "testing"

func TestFoo(t *testing.T) {

}

func TestFooFail(t *testing.T) {
	t.Fatal("failure!")
}
