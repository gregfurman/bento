package conduit

import (
	"reflect"
	"testing"
)

func TestFlattenMap(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		src      map[string]interface{}
		expected map[string]string
	}{
		{
			name:   "simple flat map",
			prefix: "",
			src: map[string]interface{}{
				"host": "localhost",
				"port": 8080,
				"ssl":  true,
			},
			expected: map[string]string{
				"host": "localhost",
				"port": "8080",
				"ssl":  "true",
			},
		},
		{
			name:   "nested map",
			prefix: "",
			src: map[string]interface{}{
				"database": map[string]interface{}{
					"host":     "localhost",
					"port":     5432,
					"username": "user",
				},
				"ssl": map[string]interface{}{
					"enabled": true,
					"cert":    "/path/to/cert",
				},
			},
			expected: map[string]string{
				"database.host":     "localhost",
				"database.port":     "5432",
				"database.username": "user",
				"ssl.enabled":       "true",
				"ssl.cert":          "/path/to/cert",
			},
		},
		{
			name:   "map with array",
			prefix: "",
			src: map[string]interface{}{
				"servers": []interface{}{"server1", "server2", "server3"},
				"ports":   []interface{}{8080, 9090},
			},
			expected: map[string]string{
				"servers.0": "server1",
				"servers.1": "server2", 
				"servers.2": "server3",
				"ports.0":   "8080",
				"ports.1":   "9090",
			},
		},
		{
			name:   "with prefix",
			prefix: "config",
			src: map[string]interface{}{
				"host": "localhost",
				"port": 8080,
			},
			expected: map[string]string{
				"config.host": "localhost",
				"config.port": "8080",
			},
		},
		{
			name:   "deeply nested",
			prefix: "",
			src: map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"level3": map[string]interface{}{
							"value": "deep",
						},
					},
				},
			},
			expected: map[string]string{
				"level1.level2.level3.value": "deep",
			},
		},
		{
			name:   "mixed types",
			prefix: "",
			src: map[string]interface{}{
				"string":  "hello",
				"int":     42,
				"float":   3.14,
				"bool":    false,
				"nil":     nil,
			},
			expected: map[string]string{
				"string": "hello",
				"int":    "42",
				"float":  "3.14", 
				"bool":   "false",
				"nil":    "<nil>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := make(map[string]string)
			flattenMap(tt.prefix, tt.src, result)
			
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("flattenMap() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFlattenMapEmpty(t *testing.T) {
	result := make(map[string]string)
	flattenMap("", map[string]interface{}{}, result)
	
	if len(result) != 0 {
		t.Errorf("flattenMap() with empty input should produce empty result, got %v", result)
	}
}