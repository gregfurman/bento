package etcd

const (
	// Common
	etcdEndpointsField   = "endpoints"
	etcdTlsField         = "tls"
	etcdOperationOptions = "options"

	// etcd common options
	etcdKeyField        = "key"
	etcdWithPrefixField = "with_prefix"

	// etcd return options
	etcdWatchWithKeysOnly = "with_keys_only"

	// etcd watch options
	etcdWatchField                   = "etcd_watch"
	etcdWatchWithProgressNotifyField = "with_progress_notify"
	etcdWatchWithCreatedNotifyField  = "with_created_notify"
	etcdWatchWithFilterPut           = "with_put_filter"
	etcdWatchWithFilterDelete        = "with_delete_filter"
	etcdWatchWithRangeField          = "with_range"

	// Auth
	etcdAuthField         = "auth"
	etcdAuthEnabledField  = "enabled"
	etcdAuthUsernameField = "username"
	etcdAuthPasswordField = "password"

	// Timeouts
	etcdDialTimeoutField     = "dial_timeout"
	etcdKeepAliveTimeField   = "keep_alive_time"
	etcdKeepAliveTimoutField = "keep_alive_timeout"
	etcdRequestTimeoutField  = "request_timeout"
)
