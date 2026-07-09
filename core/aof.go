package core

import (
	"fmt"
	"github/redis.go/config"
	"os"
	"strings"
)

func dumpKey(file *os.File, k string, obj *Obj) {
	cmd := fmt.Sprintf("SET %s %s", k, obj.Value)
	tokens := strings.Split(cmd, " ")
	file.Write(Encode(tokens, false))
}
func DumpAlLAof() error {
	file, err := os.OpenFile(config.AOFfile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	RWmutex.RLock()
	defer RWmutex.RUnlock()

	for k, obj := range store {
		dumpKey(file, k, obj)
	}
	return nil
}
