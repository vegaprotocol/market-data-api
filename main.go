package main

import (
	"fmt"
	"market-data-api/api"
)

func main() {
	fmt.Println("starting market data api")
	api.NewApi().Init()
}
