package main

import (
    "encoding/json"
    "log"
	// "sync"
	// "sync/atomic"
	"strconv"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)



func main() {

	n := maelstrom.NewNode()

	var counter int = 0
	
	// https://pkg.go.dev/github.com/jepsen-io/maelstrom/demo/go#Node.Reply
	n.Handle("generate", func(msg maelstrom.Message) error {

		body := map[string]any{}
		// Unmarshal the message body as an loosely-typed map.
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"

		node_id := n.ID() // string
		lastChar, _ := strconv.Atoi(node_id[len(node_id)-1:]) // int
		lastCharInt := lastChar + 1
 
		counterId := strconv.Itoa(lastCharInt) + strconv.Itoa(counter) 
		body["id"], _ = strconv.Atoi(counterId)

		counter += 1

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
    	log.Fatal(err)
	}
}