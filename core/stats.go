package core

var KeyspaceStat [4]map[string]int

func UpdateDBStat(num int, metric string, value int) {
	KeyspaceStat[num][metric] = value
}

// trackKeyAdded and trackKeyRemoved keep the keyspace counter in step with the
// map. Both assume the caller holds the store's write lock.
func trackKeyAdded() {
	if KeyspaceStat[0] == nil {
		KeyspaceStat[0] = make(map[string]int)
	}
	KeyspaceStat[0]["keys"]++
}

func trackKeyRemoved() {
	if KeyspaceStat[0] == nil {
		return
	}
	KeyspaceStat[0]["keys"]--
}
