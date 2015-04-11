// https://github.com/garyburd/redigo/blob/master/redis/script.go

package tophat

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"

	"github.com/fzzy/radix/extra/cluster"
	"github.com/fzzy/radix/redis"
)

type Script struct {
	keyCount int
	src      string
	hash     string
}

func NewScript(keyCount int, src string) *Script {
	h := sha1.New()
	io.WriteString(h, src)
	return &Script{keyCount, src, hex.EncodeToString(h.Sum(nil))}
}

func (s *Script) args(spec string, keysAndArgs []interface{}) []interface{} {
	var args []interface{}
	if s.keyCount < 0 {
		args = make([]interface{}, 1+len(keysAndArgs))
		args[0] = spec
		copy(args[1:], keysAndArgs)
	} else {
		args = make([]interface{}, 2+len(keysAndArgs))
		args[0] = spec
		args[1] = s.keyCount
		copy(args[2:], keysAndArgs)
	}
	return args
}

func (s *Script) Cmd(c *cluster.Cluster, keysAndArgs ...interface{}) *redis.Reply {
	reply := c.Cmd("EVALSHA", s.args(s.hash, keysAndArgs)...)
	if reply.Err != nil && strings.HasPrefix(reply.Err.Error(), "NOSCRIPT ") {
		reply = c.Cmd("EVAL", s.args(s.src, keysAndArgs)...)
	}
	return reply
}
