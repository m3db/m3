// Package validator implements value validations
//
// Copyright 2014 Roberto Teixeira <robteix@robteix.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package validator_test

import (
	"testing"

	. "gopkg.in/check.v1"
	"gopkg.in/validator.v2"
	"reflect"
)

func Test(t *testing.T) {
	TestingT(t)
}

type MySuite struct{}

var _ = Suite(&MySuite{})

type Simple struct {
	A int `validate:"min=10"`
}

type TestStruct struct {
	A   int    `validate:"nonzero"`
	B   string `validate:"len=8,min=6,max=4"`
	Sub struct {
		A int `validate:"nonzero"`
		B string
		C float64 `validate:"nonzero,min=1"`
		D *string `validate:"nonzero"`
	}
	D *Simple `validate:"nonzero"`
}

func (ms *MySuite) TestValidate(c *C) {
	t := TestStruct{
		A: 0,
		B: "12345",
	}
	t.Sub.A = 1
	t.Sub.B = ""
	t.Sub.C = 0.0
	t.D = &Simple{10}

	err := validator.Validate(t)
	c.Assert(err, NotNil)

	errs, ok := err.(validator.ErrorMap)
	c.Assert(ok, Equals, true)
	c.Assert(errs["A"], HasError, validator.ErrZeroValue)
	c.Assert(errs["B"], HasError, validator.ErrLen)
	c.Assert(errs["B"], HasError, validator.ErrMin)
	c.Assert(errs["B"], HasError, validator.ErrMax)
	c.Assert(errs["Sub.A"], HasLen, 0)
	c.Assert(errs["Sub.B"], HasLen, 0)
	c.Assert(errs["Sub.C"], HasLen, 2)
	c.Assert(errs["Sub.D"], HasError, validator.ErrZeroValue)
}

func (ms *MySuite) TestValidSlice(c *C) {
	s := make([]int, 0, 10)
	err := validator.Valid(s, "nonzero")
	c.Assert(err, NotNil)
	errs, ok := err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrZeroValue)

	for i := 0; i < 10; i++ {
		s = append(s, i)
	}

	err = validator.Valid(s, "min=11,max=5,len=9,nonzero")
	c.Assert(err, NotNil)
	errs, ok = err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrMin)
	c.Assert(errs, HasError, validator.ErrMax)
	c.Assert(errs, HasError, validator.ErrLen)
	c.Assert(errs, Not(HasError), validator.ErrZeroValue)
}

func (ms *MySuite) TestValidMap(c *C) {
	m := make(map[string]string)
	err := validator.Valid(m, "nonzero")
	c.Assert(err, NotNil)
	errs, ok := err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrZeroValue)

	err = validator.Valid(m, "min=1")
	c.Assert(err, NotNil)
	errs, ok = err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrMin)

	m = map[string]string{"A": "a", "B": "a"}
	err = validator.Valid(m, "max=1")
	c.Assert(err, NotNil)
	errs, ok = err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrMax)

	err = validator.Valid(m, "min=2, max=5")
	c.Assert(err, IsNil)

	m = map[string]string{
		"1": "a",
		"2": "b",
		"3": "c",
		"4": "d",
		"5": "e",
	}
	err = validator.Valid(m, "len=4,min=6,max=1,nonzero")
	c.Assert(err, NotNil)
	errs, ok = err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrLen)
	c.Assert(errs, HasError, validator.ErrMin)
	c.Assert(errs, HasError, validator.ErrMax)
	c.Assert(errs, Not(HasError), validator.ErrZeroValue)

}

func (ms *MySuite) TestValidFloat(c *C) {
	err := validator.Valid(12.34, "nonzero")
	c.Assert(err, IsNil)

	err = validator.Valid(0.0, "nonzero")
	c.Assert(err, NotNil)
	errs, ok := err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrZeroValue)
}

func (ms *MySuite) TestValidInt(c *C) {
	i := 123
	err := validator.Valid(i, "nonzero")
	c.Assert(err, IsNil)

	err = validator.Valid(i, "min=1")
	c.Assert(err, IsNil)

	err = validator.Valid(i, "min=124, max=122")
	c.Assert(err, NotNil)
	errs, ok := err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrMin)
	c.Assert(errs, HasError, validator.ErrMax)

	err = validator.Valid(i, "max=10")
	c.Assert(err, NotNil)
	errs, ok = err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrMax)
}

func (ms *MySuite) TestValidString(c *C) {
	s := "test1234"
	err := validator.Valid(s, "len=8")
	c.Assert(err, IsNil)

	err = validator.Valid(s, "len=0")
	c.Assert(err, NotNil)
	errs, ok := err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasError, validator.ErrLen)

	err = validator.Valid(s, "regexp=^[tes]{4}.*")
	c.Assert(err, IsNil)

	err = validator.Valid(s, "regexp=^.*[0-9]{5}$")
	c.Assert(errs, NotNil)

	err = validator.Valid("", "nonzero,len=3,max=1")
	c.Assert(err, NotNil)
	errs, ok = err.(validator.ErrorArray)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasLen, 2)
	c.Assert(errs, HasError, validator.ErrZeroValue)
	c.Assert(errs, HasError, validator.ErrLen)
	c.Assert(errs, Not(HasError), validator.ErrMax)
}

func (ms *MySuite) TestValidateStructVar(c *C) {
	// just verifies that a the given val is a struct
	validator.SetValidationFunc("struct", func(val interface{}, _ string) error {
		v := reflect.ValueOf(val)
		if v.Kind() == reflect.Struct {
			return nil
		}
		return validator.ErrUnsupported
	})

	type test struct {
		A int
	}
	err := validator.Valid(test{}, "struct")
	c.Assert(err, IsNil)

	type test2 struct {
		B int
	}
	type test1 struct {
		A test2 `validate:"struct"`
	}

	err = validator.Validate(test1{})
	c.Assert(err, IsNil)

	type test4 struct {
		B int `validate:"foo"`
	}
	type test3 struct {
		A test4
	}
	err = validator.Validate(test3{})
	errs, ok := err.(validator.ErrorMap)
	c.Assert(ok, Equals, true)
	c.Assert(errs["A.B"], HasError, validator.ErrUnknownTag)
}

func (ms *MySuite) TestValidatePointerVar(c *C) {
	// just verifies that a the given val is a struct
	validator.SetValidationFunc("struct", func(val interface{}, _ string) error {
		v := reflect.ValueOf(val)
		if v.Kind() == reflect.Struct {
			return nil
		}
		return validator.ErrUnsupported
	})
	validator.SetValidationFunc("nil", func(val interface{}, _ string) error {
		v := reflect.ValueOf(val)
		if v.IsNil() {
			return nil
		}
		return validator.ErrUnsupported
	})

	type test struct {
		A int
	}
	err := validator.Valid(&test{}, "struct")
	c.Assert(err, IsNil)

	type test2 struct {
		B int
	}
	type test1 struct {
		A *test2 `validate:"struct"`
	}

	err = validator.Validate(&test1{&test2{}})
	c.Assert(err, IsNil)

	type test4 struct {
		B int `validate:"foo"`
	}
	type test3 struct {
		A test4
	}
	err = validator.Validate(&test3{})
	errs, ok := err.(validator.ErrorMap)
	c.Assert(ok, Equals, true)
	c.Assert(errs["A.B"], HasError, validator.ErrUnknownTag)

	err = validator.Valid((*test)(nil), "nil")
	c.Assert(err, IsNil)

	type test5 struct {
		A *test2 `validate:"nil"`
	}
	err = validator.Validate(&test5{})
	c.Assert(err, IsNil)

	type test6 struct {
		A *test2 `validate:"nonzero"`
	}
	err = validator.Validate(&test6{})
	errs, ok = err.(validator.ErrorMap)
	c.Assert(ok, Equals, true)
	c.Assert(errs["A"], HasError, validator.ErrZeroValue)

	err = validator.Validate(&test6{&test2{}})
	c.Assert(err, IsNil)
}

func (ms *MySuite) TestValidateOmittedStructVar(c *C) {
	type test2 struct {
		B int `validate:"min=1"`
	}
	type test1 struct {
		A test2 `validate:"-"`
	}

	t := test1{}
	err := validator.Validate(t)
	c.Assert(err, IsNil)

	errs := validator.Valid(test2{}, "-")
	c.Assert(errs, IsNil)
}

func (ms *MySuite) TestUnknownTag(c *C) {
	type test struct {
		A int `validate:"foo"`
	}
	t := test{}
	err := validator.Validate(t)
	c.Assert(err, NotNil)
	errs, ok := err.(validator.ErrorMap)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasLen, 1)
	c.Assert(errs["A"], HasError, validator.ErrUnknownTag)
}

func (ms *MySuite) TestUnsupported(c *C) {
	type test struct {
		A int     `validate:"regexp=a.*b"`
		B float64 `validate:"regexp=.*"`
	}
	t := test{}
	err := validator.Validate(t)
	c.Assert(err, NotNil)
	errs, ok := err.(validator.ErrorMap)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasLen, 2)
	c.Assert(errs["A"], HasError, validator.ErrUnsupported)
	c.Assert(errs["B"], HasError, validator.ErrUnsupported)
}

func (ms *MySuite) TestBadParameter(c *C) {
	type test struct {
		A string `validate:"min="`
		B string `validate:"len=="`
		C string `validate:"max=foo"`
	}
	t := test{}
	err := validator.Validate(t)
	c.Assert(err, NotNil)
	errs, ok := err.(validator.ErrorMap)
	c.Assert(ok, Equals, true)
	c.Assert(errs, HasLen, 3)
	c.Assert(errs["A"], HasError, validator.ErrBadParameter)
	c.Assert(errs["B"], HasError, validator.ErrBadParameter)
	c.Assert(errs["C"], HasError, validator.ErrBadParameter)
}

type hasErrorChecker struct {
	*CheckerInfo
}

func (c *hasErrorChecker) Check(params []interface{}, names []string) (bool, string) {
	var (
		ok    bool
		slice []error
		value error
	)
	slice, ok = params[0].(validator.ErrorArray)
	if !ok {
		return false, "First parameter is not an Errorarray"
	}
	value, ok = params[1].(error)
	if !ok {
		return false, "Second parameter is not an error"
	}

	for _, v := range slice {
		if v == value {
			return true, ""
		}
	}
	return false, ""
}

func (c *hasErrorChecker) Info() *CheckerInfo {
	return c.CheckerInfo
}

var HasError = &hasErrorChecker{&CheckerInfo{Name: "HasError", Params: []string{"HasError", "expected to contain"}}}
