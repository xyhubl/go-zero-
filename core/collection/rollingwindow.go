package collection

import (
	"go-zero-/core/timex"
	"sync"
	"time"
)

// 滑动窗口

type Bucket struct {
	Sum   float64
	Count int64
}

func (b *Bucket) add(v float64) {
	b.Sum += v
	b.Count++
}

func (b *Bucket) reset() {
	b.Sum = 0
	b.Count = 0
}

// 时间窗口
type window struct {
	buckets []*Bucket // 一个桶标识一个时间间隔
	size    int       // 窗口大小
}

func newWindow(size int) *window {
	buckets := make([]*Bucket, size)
	for i := 0; i < size; i++ {
		buckets[i] = new(Bucket)
	}
	return &window{
		buckets: buckets,
		size:    size,
	}
}

func (w *window) add(offset int, v float64) {
	w.buckets[offset%w.size].add(v)
}

// 汇总数据
// fn - 自定义的bucket统计函数
func (w *window) reduce(start, count int, fn func(b *Bucket)) {
	for i := 0; i < count; i++ {
		fn(w.buckets[(start+i)%w.size])
	}
}

// 清理特定bucket
func (w *window) resetBucket(offset int) {
	w.buckets[offset%w.size].reset()
}

type (
	RollingWindow struct {
		lock sync.RWMutex
		// 滑动窗口数量
		size int
		// 窗口 数据容器
		win *window
		// 滑动窗口单元时间间隔
		interval time.Duration
		// 游标，用于定位当前应该写入哪个bucket
		offset int
		// 汇总数据时，是否忽略当前正在写入桶的数据
		// 某些场景下因为当前正在写入的桶数据并没有经过完整的窗口时间间隔 可能导致当前桶的统计并不准确
		ignoreCurrent bool
		// 最后写入桶的时间 用于计算下一次写入数据间隔最后一次写入数据的之间 经过了多少个时间间隔
		lastTime time.Duration
	}
	RollingWindowOption func(rollingWindow *RollingWindow)
)

func NewRollingWindow(size int, interval time.Duration, opts ...RollingWindowOption) *RollingWindow {
	if size < 1 {
		panic("size must be greater than 0")
	}
	w := &RollingWindow{
		size:     size,
		win:      newWindow(size),
		interval: interval,
		lastTime: timex.Now(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func (rw *RollingWindow) Add(v float64) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.updateOffset()
	rw.win.add(rw.offset, v)
}

func (rw *RollingWindow) span() int {
	// 算出经过了多少个时间单元间隔，实际上就是指经过了多少个桶
	offset := int(timex.Since(rw.lastTime) / rw.interval)
	if 0 <= offset && offset < rw.size {
		return offset
	}
	// 最大不能超过痛的数量
	return rw.size
}

func (rw *RollingWindow) updateOffset() {
	span := rw.span()
	if span <= 0 {
		return
	}
	offset := rw.offset
	// 重置过期的buckets
	for i := 0; i < span; i++ {
		// 取余操作, 把之前过期的桶清除, 因为这段时间经过了span个桶的数据,之前的数据已经无效了
		rw.win.resetBucket((offset + i + 1) % rw.size)
	}
	// 更新offset, 也就是指向当前的桶
	rw.offset = (offset + span) % rw.size
	// 更新现在的时间
	now := timex.Now()
	// 思考: 这里为什么不直接用 now - rw.lastTime
	// 如果直接使用 now - rw.lastTime，得到的是当前时间和上次更新时间之间的时间差,而我们需要根据滚动窗口的间隔来调整这个时间差，以便将下一次更新时间对齐到间隔的边界上。
	/*
		为了更好地理解`rw.lastTime`的计算过程，我们可以通过一个图文示例来说明。假设我们的`interval`是30分钟，我们来跟踪一个小时内的时间段。
		```
		时间线 (1小时): 00:00 - 01:00 - 02:00 - 03:00 - 04:00
		```
		现在，假设`rw.lastTime`是01:30，这意味着上一个桶的开始时间是01:00。我们想要将`rw.lastTime`对齐到下一个30分钟的整数倍，也就是02:00。
		下面是计算过程的步骤：
		1. 首先，我们计算从`rw.lastTime`（01:00）到当前时间`now`（假设为02:15）的持续时间：
		   `02:15 - 01:00 = 1小时15分钟`
		2. 接下来，我们计算这个持续时间与`interval`（30分钟）的余数：
		   `1小时15分钟 % 30分钟 = 15分钟`
		3. 现在我们知道，从`rw.lastTime`开始的1小时15分钟内有15分钟没有被包含在一个完整的30分钟桶内。为了对齐到下一个30分钟的整数倍，我们需要从当前时间`now`减去这15分钟：
		   `02:15 - 15分钟 = 02:00`
		4. 因此，我们将`rw.lastTime`更新为02:00，这是下一个30分钟桶的开始时间。
		用图表示如下：
		```
		时间线 (1小时): 00:00 - 01:00 - 02:00 - 03:00 - 04:00
		                ↑
		                lastTime (01:00) - 现在对齐到 02:00
		```
		通过这种方式，我们确保了每个桶都是完整且等长的，便于我们进行统计和分析。
	*/
	rw.lastTime = now - (now-rw.lastTime)%rw.interval
}

func (rw *RollingWindow) Reduce(fn func(b *Bucket)) {
	rw.lock.RLock()
	defer rw.lock.RUnlock()

	var diff int

	span := rw.span()
	if span == 0 && rw.ignoreCurrent {
		diff = rw.size - 1
	} else {
		diff = rw.size - span
	}
	if diff > 0 {
		offset := (rw.offset + span + 1) % rw.size
		rw.win.reduce(offset, diff, fn)
	}
}

func IgnoreCurrentBucket() RollingWindowOption {
	return func(w *RollingWindow) {
		w.ignoreCurrent = true
	}
}
