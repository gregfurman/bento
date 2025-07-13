package conduit

import (
	"fmt"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	conduitPluginField   = "plugin"
	conduitIdField       = "id"
	conduitSettingsField = "settings"
)

type conduitConfig struct {
	plugin   string
	settings map[string]string
}

func connectorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary("Runs Conduit connectors as source or destination components.").
		Fields(
			service.NewStringField(conduitPluginField),
			service.NewStringField(conduitIdField).Description("No-op for backwards compatability with Conduit config. Use `label` instead.").Default(""), // for backwards compatability
			service.NewAnyMapField(conduitSettingsField).Optional(),
		)
}

func parseConfig(pconf *service.ParsedConfig) (*conduitConfig, error) {
	conf := conduitConfig{
		settings: make(map[string]string),
	}

	var err error
	if conf.plugin, err = pconf.FieldString(conduitPluginField); err != nil {
		return nil, err
	}

	if pconf.Contains(conduitSettingsField) {
		configMap, err := pconf.FieldAny(conduitSettingsField)
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
		default:
			dest[prefix+k] = fmt.Sprintf("%v", v)
		}
	}
}
