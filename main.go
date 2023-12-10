package main

import (
	"fastdb/config"
	"fastdb/fastdb"
)

var banner = `
___________                __ ________ __________ 
\_   _____/____    _______/  |\______ \\______   \
 |    __) \__  \  /  ___/\   __\    |  \|    |  _/
 |     \   / __ \_\___ \  |  | |       \    |   \
\___  /  (____  /____  > |__|/_______  /______  /
\/        \/     \/              \/       \/
`

func main() {
	print(banner)
	options := config.ServerOptions{
		DbOptions:    config.DefaultOptions,
		BatchOptions: config.DefaultBatchOptions,
		Port:         6666,
	}
	fastDB, err := fastdb.MakeServer(options)
	if err != nil {
		print(err)
	}

	err = fastDB.Run()
	defer fastDB.Close()

	if err != nil {
		print(err)
	}
}
