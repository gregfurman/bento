//go:build huggingbento

package huggingface

import (
	"sync"

	"github.com/knights-analytics/hugot"
)

type ortSession struct {
	mut     sync.Mutex
	session *hugot.Session
}

func (o *ortSession) Get() *hugot.Session {
	o.mut.Lock()
	session := o.session
	o.mut.Unlock()
	return session
}

func (o *ortSession) Destroy() {
	o.mut.Lock()
	defer o.mut.Unlock()
	o.session.Destroy()
}

func (o *ortSession) NewSession(onnxLibraryPath string) (*hugot.Session, error) {
	o.mut.Lock()
	defer o.mut.Unlock()

	if o.session == nil {
		session, err := hugot.NewSession(hugot.WithOnnxLibraryPath(onnxLibraryPath))
		if err != nil {
			return nil, err
		}
		o.session = session
	}

	return o.session, nil
}

var globalSession = &ortSession{
	session: nil,
	mut:     sync.Mutex{},
}
