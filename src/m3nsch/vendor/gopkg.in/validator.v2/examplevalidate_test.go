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
	"fmt"
	"sort"

	"gopkg.in/validator.v2"
)

// This example demonstrates a custom function to process template text.
// It installs the strings.Title function and uses it to
// Make Title Text Look Good In Our Template's Output.
func ExampleValidate() {
	// First create a struct to be validated
	// according to the validator tags.
	type ValidateExample struct {
		Name        string `validate:"nonzero"`
		Description string
		Age         int    `validate:"min=18"`
		Email       string `validate:"regexp=^[0-9a-z]+@[0-9a-z]+(\\.[0-9a-z]+)+$"`
		Address     struct {
			Street string `validate:"nonzero"`
			City   string `validate:"nonzero"`
		}
	}

	// Fill in some values
	ve := ValidateExample{
		Name:        "Joe Doe", // valid as it's nonzero
		Description: "",        // valid no validation tag exists
		Age:         17,        // invalid as age is less than required 18
	}
	// invalid as Email won't match the regular expression
	ve.Email = "@not.a.valid.email"
	ve.Address.City = "Some City" // valid
	ve.Address.Street = ""        // invalid

	err := validator.Validate(ve)
	if err == nil {
		fmt.Println("Values are valid.")
	} else {
		errs := err.(validator.ErrorMap)
		// See if Address was empty
		if errs["Address.Street"][0] == validator.ErrZeroValue {
			fmt.Println("Street cannot be empty.")
		}

		// Iterate through the list of fields and respective errors
		fmt.Println("Invalid due to fields:")

		// Here we have to sort the arrays to ensure map ordering does not
		// fail our example, typically it's ok to just range through the err
		// list when order is not important.
		var errOuts []string
		for f, e := range errs {
			errOuts = append(errOuts, fmt.Sprintf("\t - %s (%v)\n", f, e))
		}

		// Again this part is extraneous and you should not need this in real
		// code.
		sort.Strings(errOuts)
		for _, str := range errOuts {
			fmt.Print(str)
		}
	}

	// Output:
	// Street cannot be empty.
	// Invalid due to fields:
	//	 - Address.Street (zero value)
	// 	 - Age (less than min)
	// 	 - Email (regular expression mismatch)
}

// This example shows how to use the Valid helper
// function to validator any number of values
func ExampleValid() {
	err := validator.Valid(42, "min=10,max=100,nonzero")
	fmt.Printf("42: valid=%v, errs=%v\n", err == nil, err)

	var ptr *int
	if err := validator.Valid(ptr, "nonzero"); err != nil {
		fmt.Println("ptr: Invalid nil pointer.")
	}

	err = validator.Valid("ABBA", "regexp=[ABC]*")
	fmt.Printf("ABBA: valid=%v\n", err == nil)

	// Output:
	// 42: valid=true, errs=<nil>
	// ptr: Invalid nil pointer.
	// ABBA: valid=true
}

// This example shows you how to change the tag name
func ExampleSetTag() {
	type T struct {
		A int `foo:"nonzero" bar:"min=10"`
	}
	t := T{5}
	v := validator.NewValidator()
	v.SetTag("foo")
	err := v.Validate(t)
	fmt.Printf("foo --> valid: %v, errs: %v\n", err == nil, err)
	v.SetTag("bar")
	err = v.Validate(t)
	errs := err.(validator.ErrorMap)
	fmt.Printf("bar --> valid: %v, errs: %v\n", err == nil, errs)

	// Output:
	// foo --> valid: true, errs: <nil>
	// bar --> valid: false, errs: A: less than min
}

// This example shows you how to change the tag name
func ExampleWithTag() {
	type T struct {
		A int `foo:"nonzero" bar:"min=10"`
	}
	t := T{5}
	err := validator.WithTag("foo").Validate(t)
	fmt.Printf("foo --> valid: %v, errs: %v\n", err == nil, err)
	err = validator.WithTag("bar").Validate(t)
	fmt.Printf("bar --> valid: %v, errs: %v\n", err == nil, err)

	// Output:
	// foo --> valid: true, errs: <nil>
	// bar --> valid: false, errs: A: less than min
}
