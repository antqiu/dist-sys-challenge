package main

import (
    "encoding/json"
    "log"
	"sync"
	"sync/atomic"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)



func main() {

	n := maelstrom.NewNode()
	var id atomic.Uint64
	var wg sync.WaitGroup

	doIncrement := func() {
		id.Add(1)
	}
	
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any = {}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "generate_ok"

		wg.Go(doIncrement)
		wg.Wait()
		
		body["id"] = id.LoadUint64()

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
    	log.Fatal(err)
	}
}