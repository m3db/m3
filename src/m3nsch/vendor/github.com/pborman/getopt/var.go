// Copyright 2013 Google Inc.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package getopt

import (
	"fmt"
	"os"
	"runtime"
)

// Value is the interface to the dynamic value stored in a flag. (The default
// value is represented as a string.)  Set is passed the string to set the
// value to as well as the Option that is being processed.
type Value interface {
	Set(string, Option) error
	String() string
}

// Var creates an option of the specified name. The type and value of the option
// are represented by the first argument, of type Value, which typically holds a
// user-defined implementation of Value.  All options are ultimately created
// as a Var.
func Var(p Value, name rune, helpvalue ...string) Option {
	return CommandLine.VarLong(p, "", name, helpvalue...)
}

func VarLong(p Value, name string, short rune, helpvalue ...string) Option {
	return CommandLine.VarLong(p, name, short, helpvalue...)
}

func (s *Set) Var(p Value, name rune, helpvalue ...string) Option {
	return s.VarLong(p, "", name, helpvalue...)
}

func (s *Set) VarLong(p Value, name string, short rune, helpvalue ...string) Option {
	opt := &option{
		short:  short,
		long:   name,
		value:  p,
		defval: p.String(),
	}

	switch len(helpvalue) {
	case 2:
		opt.name = helpvalue[1]
		fallthrough
	case 1:
		opt.help = helpvalue[0]
	case 0:
	default:
		panic("Too many strings for String helpvalue")
	}
	if _, file, line, ok := runtime.Caller(1); ok {
		opt.where = fmt.Sprintf("%s:%d", file, line)
	}
	if opt.short == 0 && opt.long == "" {
		fmt.Fprintf(os.Stderr, opt.where+": no short or long option given")
		os.Exit(1)
	}
	if opt.short != 0 {
		if oo, ok := s.shortOptions[opt.short]; ok {
			fmt.Fprintf(os.Stderr, "%s: -%c already declared at %s", opt.where, opt.short, oo.where)
			os.Exit(1)
		}
		s.shortOptions[opt.short] = opt
	}
	if opt.long != "" {
		if oo, ok := s.longOptions[opt.long]; ok {
			fmt.Fprintf(os.Stderr, "%s: --%s already declared at %s", opt.where, opt.long, oo.where)
			os.Exit(1)
		}
		s.longOptions[opt.long] = opt
	}
	s.options = append(s.options, opt)
	return opt
}
