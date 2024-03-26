package breaker

import (
	"go-zero-/core/collection"
	"go-zero-/core/mathx"
	"time"
)

const (
	window     = time.Second * 10
	buckets    = 40
	k          = 1.5
	protection = 5
)

type googleBreaker struct {
	// 敏感度
	k float64
	// 滑动窗口
	stat *collection.RollingWindow
	// 概率生成器 0.0 - 1.0 之间
	proba *mathx.Proba
}

func newGoogleBreaker() *googleBreaker {
	bucketDuration := time.Duration(int64(window) / int64(buckets))
	st := collection.NewRollingWindow(buckets, bucketDuration)
	return &googleBreaker{
		stat:  st,
		k:     k,
		proba: mathx.NewProba(),
	}
}

func (b *googleBreaker) accept() error {
	accepts, total := b.history()

	weightedAccepts := b.k + float64(accepts)
	dropRatio := (float64(total-protection) - weightedAccepts) / float64(total+1)
	if dropRatio <= 0 {
		return nil
	}
	if b.proba.TrueOnProba(dropRatio) {
		return ErrServiceUnavailable
	}
	return nil
}

func (b *googleBreaker) history() (accepts, total int64) {
	b.stat.Reduce(func(b *collection.Bucket) {
		accepts += int64(b.Sum)
		total += b.Count
	})
	return
}
