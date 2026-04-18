// Package config holds shared configuration defaults for hearthstore.
package config

import "time"

// HeartbeatInterval is how often streaming RPCs send keep-alive messages to
// prevent WebChannel long-poll connections from being dropped by the browser
// or intermediate proxies before the next real event arrives.
// Tests may override this to a short duration without affecting production.
var HeartbeatInterval = 5 * time.Second

// SessionIdleTimeout is how long a WebChannel session can go without any
// client activity (GET poll or subsequent POST) before the server cancels it.
// Matches Firebase's own server-side behaviour of reaping abandoned sessions.
// Tests may override this to a short duration.
var SessionIdleTimeout = 5 * time.Minute
