package bloblang

import (
	"errors"

	ibloblang "github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bundle"
)

func init() {
	env := ibloblang.GlobalEnvironment()

	// Register resourced plugins
	registerCacheBloblangPlugins(env)
}

//------------------------------------------------------------------------------

var getNewManagement = func() (bundle.NewManagement, error) {
	return nil, errors.New("Management bundle was never initialised to the environment.")
}

func SetManagementSingleton(mgr bundle.NewManagement) {
	getNewManagement = func() (bundle.NewManagement, error) {
		return mgr, nil
	}
}
