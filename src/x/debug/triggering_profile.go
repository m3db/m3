// Copyright (c) 2020 Uber Technologies, Inc.
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

package debug

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime/pprof"
	"text/template"
	"time"

	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

const (
	// ContinuousCPUProfileName is the name of continuous CPU profile.
	ContinuousCPUProfileName = "cpu"

	continuousProfileBackoff = 2 * time.Minute
)

var (
	defaultConditional     = func() bool { return true }
	defaultInterval        = time.Second
	errNoFilePathTemplate  = errors.New("no file path template")
	errNoProfileName       = errors.New("no profile name")
	errNoInstrumentOptions = errors.New("no instrument options")
	errAlreadyOpen         = errors.New("already open")
	errNotOpen             = errors.New("not open")
)

// ContinuousFileProfile is a profile that runs continously
// to a file using a template for a file name.
type ContinuousFileProfile struct {
	filePathTemplate *template.Template
	profileName      string
	profileDuration  time.Duration
	profileDebug     int
	conditional      func() bool
	interval         time.Duration

	logger *zap.Logger

	closeCh chan struct{}
}

// ContinuousFileProfileOptions is a set of continuous file profile options.
type ContinuousFileProfileOptions struct {
	FilePathTemplate  string
	ProfileName       string
	ProfileDuration   time.Duration
	ProfileDebug      int
	Conditional       func() bool
	Interval          time.Duration
	InstrumentOptions instrument.Options
}

// ContinuousFileProfilePathParams is the params used to construct
// a file path.
type ContinuousFileProfilePathParams struct {
	ProfileName string
	UnixTime    int64
}

// NewContinuousFileProfile returns a new continuous file profile.
func NewContinuousFileProfile(
	opts ContinuousFileProfileOptions,
) (*ContinuousFileProfile, error) {
	if opts.FilePathTemplate == "" {
		return nil, errNoFilePathTemplate
	}
	if opts.ProfileName == "" {
		return nil, errNoProfileName
	}
	if opts.Conditional == nil {
		opts.Conditional = defaultConditional
	}
	if opts.Interval == 0 {
		opts.Interval = defaultInterval
	}
	if opts.InstrumentOptions == nil {
		return nil, errNoInstrumentOptions
	}

	tmpl, err := template.New("fileName").Parse(opts.FilePathTemplate)
	if err != nil {
		return nil, err
	}

	return &ContinuousFileProfile{
		filePathTemplate: tmpl,
		profileName:      opts.ProfileName,
		profileDuration:  opts.ProfileDuration,
		profileDebug:     opts.ProfileDebug,
		conditional:      opts.Conditional,
		interval:         opts.Interval,
		logger:           opts.InstrumentOptions.Logger(),
	}, nil
}

// Start will start the continuous file profile.
func (c *ContinuousFileProfile) Start() error {
	if c.closeCh != nil {
		return errAlreadyOpen
	}

	c.closeCh = make(chan struct{})
	go c.run()
	return nil
}

// Stop will stop the continuous file profile.
func (c *ContinuousFileProfile) Stop() error {
	if c.closeCh == nil {
		return errNotOpen
	}

	close(c.closeCh)
	c.closeCh = nil

	return nil
}

func (c *ContinuousFileProfile) run() {
	closeCh := c.closeCh

	for {
		select {
		case <-closeCh:
			return
		case <-time.After(c.interval):
			if !c.conditional() {
				continue
			}

			err := c.profile()
			if err != nil {
				c.logger.Error("continuous profile error",
					zap.String("name", c.profileName),
					zap.Int("debug", c.profileDebug),
					zap.Duration("interval", c.interval),
					zap.Error(err))
			}

			// Backoff to avoid profiling impacting performance too frequently.
			time.Sleep(continuousProfileBackoff)
		}
	}
}

func (c *ContinuousFileProfile) profile() error {
	filePathBuffer := bytes.NewBuffer(nil)
	filePathParams := ContinuousFileProfilePathParams{
		ProfileName: c.profileName,
		UnixTime:    time.Now().Unix(),
	}
	err := c.filePathTemplate.Execute(filePathBuffer, filePathParams)
	if err != nil {
		return err
	}

	w, err := os.Create(filePathBuffer.String())
	if err != nil {
		return err
	}

	success := false
	defer func() {
		if !success {
			_ = w.Close()
		}
	}()

	switch c.profileName {
	case ContinuousCPUProfileName:
		if err := pprof.StartCPUProfile(w); err != nil {
			return err
		}
		time.Sleep(c.profileDuration)
		pprof.StopCPUProfile()
	default:
		p := pprof.Lookup(c.profileName)
		if p == nil {
			return fmt.Errorf("unknown profile: %s", c.profileName)
		}
		if err := p.WriteTo(w, c.profileDebug); err != nil {
			return err
		}
	}

	success = true
	return w.Close()
}
