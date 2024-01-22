package logging

import (
	"github.com/withmandala/go-log"
	"os"
)

var Logger = log.New(os.Stderr).WithColor()

func Panic(message string) {
	Logger.Fatal(message)
}
