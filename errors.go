package xk6pubsub

import "fmt"

func ReportError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
	}
}
