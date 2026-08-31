package core

import (
	"github/redis.go/config"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// dumpKey writes one key back as the SET that would recreate it. The tokens are
// built as a slice rather than by splitting a formatted string, so values that
// contain spaces survive the round trip.
func dumpKey(w io.Writer, k string, obj *Obj) {
	value, ok := obj.Value.(string)
	if !ok {
		return
	}

	tokens := []string{"SET", k, value}
	if obj.ExpiresAt != -1 {
		leftSec := (obj.ExpiresAt - time.Now().UnixMilli()) / 1000
		if leftSec <= 0 {
			// Already expired; there is nothing worth persisting.
			return
		}
		tokens = append(tokens, "EX", strconv.FormatInt(leftSec, 10))
	}
	w.Write(Encode(tokens, false))
}

func DumpAlLAof() error {
	file, err := os.OpenFile(config.AOFfile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Println("AOF rewrite failed:", err)
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

func LoadAof() {
	fileContent, err := os.ReadFile(config.AOFfile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Println("Error reading AOF file:", err)
		}
		return
	}
	if len(fileContent) == 0 {
		return
	}

	entries, err := Decode(fileContent)
	if err != nil {
		log.Println("Error decoding AOF file:", err)
		return
	}

	restored := 0
	for _, v := range entries {
		arr, ok := v.([]interface{})
		if !ok || len(arr) == 0 {
			continue
		}
		args, err := DecodeArrayString(arr)
		if err != nil {
			log.Println("Skipping malformed AOF entry:", err)
			continue
		}
		// Replay through the same dispatch table live commands use, so a
		// restored key lands in exactly the state a fresh SET would produce.
		if handler, ok := commands[strings.ToUpper(args[0])]; ok {
			handler(args[1:])
			restored++
		}
	}
	log.Printf("Loaded AOF file, restored %d keys", restored)
}
