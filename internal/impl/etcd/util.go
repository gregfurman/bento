package etcd

import (
	"encoding/json"
	"fmt"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func marshalEtcdEventsToJSON(events []*clientv3.Event) ([]byte, error) {
	var eventsJSON []struct {
		EventType string           `json:"event_type"`
		KeyValue  *mvccpb.KeyValue `json:"key_value,omitempty"`
	}

	for _, e := range events {
		var eventType string
		switch e.Type {
		case clientv3.EventTypePut:
			eventType = "PUT"
		case clientv3.EventTypeDelete:
			eventType = "DELETE"
		default:
			return nil, fmt.Errorf("unknown event type: %d", e.Type)
		}

		eventsJSON = append(eventsJSON, struct {
			EventType string           `json:"event_type"`
			KeyValue  *mvccpb.KeyValue `json:"key_value,omitempty"`
		}{
			EventType: eventType,
			KeyValue:  e.Kv,
		})
	}

	return json.Marshal(eventsJSON)
}
