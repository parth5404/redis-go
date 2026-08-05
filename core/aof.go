package core

import (
	"fmt"
	"github/redis.go/config"
	"log"
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

func LoadAof() {
	fileContent, err := os.ReadFile(config.AOFfile)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Println("Error reading AOF file:", err)
		return
	}
	if len(fileContent) == 0 {
		return
	}

	commands, err := Decode(fileContent)
	if err != nil {
		log.Println("Error decoding AOF file:", err)
		return
	}

	for _, v := range commands {
		arr, ok := v.([]interface{})
		if !ok || len(arr) == 0 {
			continue
		}

		var args = make([]string, len(arr))
		for i := 0; i < len(arr); i++ {
			args[i] = arr[i].(string)
		}

		cmd := args[0]
		if strings.ToUpper(cmd) == "SET" {
			evalSET(args[1:])
		}
	}
	log.Println("Successfully loaded AOF file. Store populated.")
}
