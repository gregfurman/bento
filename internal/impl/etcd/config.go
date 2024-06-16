package etcd

const (
	// Common
	etcdEndpointsField   = "endpoints"
	etcdTlsField         = "tls"
	etcdOperationOptions = "options"

	// etcd watch options
	etcdWatchField                   = "etcd_watch"
	etcdWatchKeyField                = "key"
	etcdWatchWithPrefixField         = "with_prefix"
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
