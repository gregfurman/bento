package conduit

import (
	"fmt"
	"strconv"

	"github.com/warpstreamlabs/bento/public/service"
)

type conduitConfig struct {
	id       string
	plugin   string
	settings map[string]string
}

func connectorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary("Runs Conduit connectors as source or destination components.").
		Fields(
			service.NewStringField("plugin"),
			service.NewStringField("id"),
			service.NewAnyMapField("settings").Optional(),
		)
}

func parseConfig(pconf *service.ParsedConfig) (*conduitConfig, error) {
	conf := conduitConfig{
		settings: make(map[string]string),
	}

	var err error
	if conf.plugin, err = pconf.FieldString("plugin"); err != nil {
		return nil, err
	}

	if conf.id, err = pconf.FieldString("id"); err != nil {
		return nil, err
	}

	if pconf.Contains("settings") {
		configMap, err := pconf.FieldAny("settings")
		if err != nil {
			return nil, err
		}

		switch m := configMap.(type) {
		case map[string]string:
			for k, v := range m {
				conf.settings[k] = v
			}
		case map[string]interface{}:
			flattenMap("", m, conf.settings)
		}
	}

	return &conf, nil
}

func flattenMap(prefix string, src map[string]interface{}, dest map[string]string) {
	if len(prefix) > 0 {
		prefix += "."
	}
	for k, v := range src {
		switch child := v.(type) {
		case map[string]interface{}:
			flattenMap(prefix+k, child, dest)
		case []interface{}:
			for i := 0; i < len(child); i++ {
				dest[prefix+k+"."+strconv.Itoa(i)] = fmt.Sprintf("%v", child[i])
			}
		default:
			dest[prefix+k] = fmt.Sprintf("%v", v)
		}
	}
}
