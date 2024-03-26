package breaker

import (
	"errors"
	"fmt"
	"go-zero-/core/mathx"
	"go-zero-/core/proc"
	"go-zero-/core/stat"
	"go-zero-/core/stringx"
	"strings"
	"sync"
	"time"
)

const (
	numHistoryReasons = 5
	timeFormat        = "15:04:05"
)

var ErrServiceUnavailable = errors.New("circuit breaker is open")

type (
	// Acceptable 自定义判定执行结果
	Acceptable func(err error) bool

	// Promise 手动回调
	Promise interface {
		// 请求成功
		Accept()
		// 请求失败
		Reject(reason string)
	}

	internalPromise interface {
		Accept()
		Reject()
	}

	// Breaker 熔断器接口
	Breaker interface {

		// 名字
		Name() string

		// 熔断方法, 执行请求时必须手动上报执行结果
		// 适用于简单无需自定义快速失败, 无需自定义判定请求结果的场景 手动挡
		Allow()

		// 熔断方法, 自动上报结果 自动挡
		Do(req func() error) error

		// 熔断方法 支持自定义判定执行结果
		DoWithAcceptable(req func() error, acceptable Acceptable) error

		// 熔断方法 支持自定义快速失败
		DoWithFallback(req func() error, fallback func(err error) error) error

		// 熔断方法 支持自定义判定执行结果   支持自定义快速失败
		DoWithFallbackAcceptable(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}

	throttle interface {
		// 熔断
		allow() (Promise, error)
		// 熔断方法, DoXXX最终都是执行该方法
		doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}

	internalThrottle interface {
		allow() (internalPromise, error)
		doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}

	// circuitBreaker 熔断器接口
	circuitBreaker struct {
		name string
		throttle
	}
	Option func(breaker *circuitBreaker)

	Fallback func(err error) error
)

func NewBreaker(opts ...Option) Breaker {
	var b circuitBreaker
	for _, opt := range opts {
		opt(&b)
	}
	if len(b.name) == 0 {
		b.name = stringx.Rand()
	}
	return nil
}

func (cb *circuitBreaker) Name() string {
	return cb.name
}
func (cb *circuitBreaker) Allow() (Promise, error) {
	return cb.throttle.allow()
}

func (cb *circuitBreaker) Do(req func() error) error {
	return cb.throttle.doReq(req, nil, defaultAcceptable)
}

func (cb *circuitBreaker) DoWithAcceptable(req func() error, acceptable Acceptable) error {
	return cb.throttle.doReq(req, nil, acceptable)
}

func (cb *circuitBreaker) DoWithFallback(req func() error, fallback Fallback) error {
	return cb.throttle.doReq(req, fallback, defaultAcceptable)
}

func (cb *circuitBreaker) DoWithFallbackAcceptable(req func() error, fallback Fallback,
	acceptable Acceptable) error {
	return cb.throttle.doReq(req, fallback, acceptable)
}

func defaultAcceptable(err error) bool {
	return err == nil
}

type loggedThrottle struct {
	name string
	internalThrottle
	errWin *errorWindow
}

func newLoggedThrottle(name string, t internalThrottle) loggedThrottle {
	return loggedThrottle{
		name:             name,
		internalThrottle: t,
		errWin:           new(errorWindow),
	}
}

func (lt loggedThrottle) allow() (Promise, error) {
	promise, err := lt.internalThrottle.allow()
	return PromiseWithReason{
		promise: promise,
		errWin:  lt.errWin,
	}, lt.logError(err)
}

func (lt loggedThrottle) doReq(req func() error, fallback Fallback, acceptable Acceptable) error {
	return lt.logError(lt.internalThrottle.doReq(req, fallback, func(err error) bool {
		accept := acceptable(err)
		if !accept && err != nil {
			lt.errWin.add(err.Error())
		}
		return accept
	}))
}

func (lt loggedThrottle) logError(err error) error {
	if errors.Is(err, ErrServiceUnavailable) {
		stat.Report(fmt.Sprintf("proc(%s/%d), callee: %s, breaker is open and requests dropped\nlast errors:\n%s",
			proc.ProcessName(), proc.Pid(), lt.name, lt.errWin))
	}
	return err
}

// 错误窗口记录
type errorWindow struct {
	reasons [numHistoryReasons]string
	index   int
	count   int
	lock    sync.Mutex
}

func (ew *errorWindow) add(reason string) {
	ew.lock.Lock()
	ew.reasons[ew.index] = fmt.Sprintf("%s %s", time.Now().Format(timeFormat), reason)
	ew.index = (ew.index + 1) % numHistoryReasons
	ew.count = mathx.MinInt(ew.count+1, numHistoryReasons)
	ew.lock.Unlock()
}

func (ew *errorWindow) String() string {
	var reasons []string
	ew.lock.Lock()
	for i := ew.index - 1; i >= ew.index-ew.count; i-- {
		reasons = append(reasons, ew.reasons[(i+numHistoryReasons)%numHistoryReasons])
	}
	ew.lock.Unlock()
	return strings.Join(reasons, "\n")
}

// 在请求被拒绝时, 记录拒绝的原因， 并将错误信息添加到错误的窗口中
type PromiseWithReason struct {
	promise internalPromise
	errWin  *errorWindow
}

func (p PromiseWithReason) Accept() {
	p.promise.Accept()
}

func (p PromiseWithReason) Reject(reason string) {
	p.errWin.add(reason)
	p.promise.Reject()
}
