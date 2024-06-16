package etcd

const (
	// Common
	endpointsField = "endpoints"
	tlsField       = "tls"

	// etcd watch options
	watchField                   = "watch"
	watchKeyField                = "key"
	watchWithPrefixField         = "with_prefix"
	watchWithProgressNotifyField = "with_progress_notify"
	watchWithCreatedNotifyField  = "with_created_notify"
	watchWithFilterPut           = "with_put_filter"
	watchWithFilterDelete        = "with_delete_filter"
	watchWithRangeField          = "with_range"

	// Auth
	authField         = "auth"
	authEnabledField  = "enabled"
	authUsernameField = "username"
	authPasswordField = "password"

	// Timeouts
	dialTimeoutField     = "dial_timeout"
	keepAliveTimeField   = "keep_alive_time"
	keepAliveTimoutField = "keep_alive_timeout"
	requestTimeoutField  = "request_timeout"
)
