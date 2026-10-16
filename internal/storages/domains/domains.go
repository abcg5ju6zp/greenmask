package domains

import "time"

type ObjectStat struct {
	Name         string
	LastModified time.Time
	Size         int64
	Exist        bool
}
