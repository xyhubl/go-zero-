package stringx

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // 62个字符
	letterIdxBits = 6                                                                // 6位 意味着可以表示0-63的整数, 对应letterBytes的字符索引

	idLen          = 8 // 这个常量定义了生成的ID字符串的长度，设置为8个字节
	defaultRandLen = 8 // 默认随机字符串长度

	letterIdxMask = 1<<letterIdxBits - 1 // 掩码 用于提取Int63()方法 生成的63位随机数中的最低6位。由于letterIdxBits是6，1<<letterIdxBits - 1计算得到的是0x3F（即二进制的0011 1111），它可以与随机数进行按位与操作（&），以获取一个0到63范围内的索引
	letterIdxMax  = 63 / letterIdxBits   // 63位随机数可以表示多少个字符索引
)

var src = newLockedSource(time.Now().UnixNano())

// 关于为什么要加锁 https://aptxx.com/posts/golang-rand-concurrency-safe/
type lockSource struct {
	lock   sync.Mutex
	source rand.Source
}

func newLockedSource(seed int64) *lockSource {
	return &lockSource{
		source: rand.NewSource(seed),
	}
}

func (ls *lockSource) Int63() int64 {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	return ls.source.Int63()
}

func (ls *lockSource) Seed(seed int64) {
	ls.lock.Lock()
	defer ls.lock.Unlock()
	ls.source.Seed(seed)
}

func Randn(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMask; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

func RandId() string {
	b := make([]byte, idLen)
	_, err := crand.Read(b)
	if err != nil {
		return Randn(idLen)
	}

	return fmt.Sprintf("%x%x%x%x", b[0:2], b[2:4], b[4:6], b[6:8])
}

func Rand() string {
	return Randn(defaultRandLen)
}

func Seed(seed int64) {
	src.Seed(seed)
}
