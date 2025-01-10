package bundle

import (
	"fmt"
	"sort"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/retry"
	"github.com/warpstreamlabs/bento/internal/docs"
)

// AllRetries is a set containing every single retry that has been imported.
var AllRetries = &RetrySet{
	specs: map[string]retrySpec{},
}

//------------------------------------------------------------------------------

// RetryAdd adds a new retry to this environment by providing a
// constructor and documentation.
func (e *Environment) RetryAdd(constructor RetryConstructor, spec docs.ComponentSpec) error {
	return e.retries.Add(constructor, spec)
}

// RetryInit attempts to initialise a retry from a config.
func (e *Environment) RetryInit(conf retry.Config, mgr NewManagement) (retry.V1, error) {
	return e.retries.Init(conf, mgr)
}

// RetryDocs returns a slice of retry specs, which document each method.
func (e *Environment) RetryDocs() []docs.ComponentSpec {
	return e.retries.Docs()
}

//------------------------------------------------------------------------------

// RetryConstructor constructs an retry component.
type RetryConstructor func(retry.Config, NewManagement) (retry.V1, error)

type retrySpec struct {
	constructor RetryConstructor
	spec        docs.ComponentSpec
}

// RetrySet contains an explicit set of retries available to a Bento service.
type RetrySet struct {
	specs map[string]retrySpec
}

// Add a new retry to this set by providing a spec (name, documentation, and
// constructor).
func (s *RetrySet) Add(constructor RetryConstructor, spec docs.ComponentSpec) error {
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("component name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if s.specs == nil {
		s.specs = map[string]retrySpec{}
	}
	spec.Type = docs.TypeRetry
	s.specs[spec.Name] = retrySpec{
		constructor: constructor,
		spec:        spec,
	}
	return nil
}

// Init attempts to initialise an retry from a config.
func (s *RetrySet) Init(conf retry.Config, mgr NewManagement) (retry.V1, error) {
	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, component.ErrInvalidType("retry", conf.Type)
	}
	c, err := spec.constructor(conf, mgr)
	err = wrapComponentErr(mgr, "retry", err)
	return c, err
}

// Docs returns a slice of retry specs, which document each method.
func (s *RetrySet) Docs() []docs.ComponentSpec {
	var docs []docs.ComponentSpec
	for _, v := range s.specs {
		docs = append(docs, v.spec)
	}
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].Name < docs[j].Name
	})
	return docs
}

// DocsFor returns the documentation for a given component name, returns a
// boolean indicating whether the component name exists.
func (s *RetrySet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
