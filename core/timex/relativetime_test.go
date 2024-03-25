package timex

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRelativeTime(t *testing.T) {
	now := Now()
	assert.True(t, now > 0)

	interval := time.Second * 5 / time.Duration(50)

	sinceTime := time.Since(initTime)
	for i := 0; i < 300; i++ {
		fmt.Println(interval, Since(sinceTime), int(Since(sinceTime)/interval), i, 300%299)
		time.Sleep(time.Millisecond)
	}
}
