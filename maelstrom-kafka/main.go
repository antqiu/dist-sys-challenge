package main

import (
    "encoding/json"
    "log"
	// "time"
	// "math/rand"
	"sync"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	logs := make(map[string][][]int)
	committed := make(map[string]map[string]int)
	//   "offsets": {
//     "k1": 1000,
//     "k2": 2000
//   }
	
	var mu sync.Mutex

	n.Handle("send", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		response := make(map[string]any)
		response["type"] = "send_ok"
	
		messageRaw, _ := body["message"].(float64)
		message := int(messageRaw)

		key, _ := body["key"].(string)
		
		mu.Lock()
		defer mu.Unlock()
		
		nextOffset := len(logs[key]) + 1
		logs[key] = append(logs[key], []int{nextOffset, message})
		response["offset"] = nextOffset
		
		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		response := make(map[string]any)
		response["type"] = "poll_ok"

		returnedMessages := make(map[string][][]int) // messages thhat start afetr the offset
		
		offsets, _ := body["offsets"].(map[string]any)

		mu.Lock()
		defer mu.Unlock()

		for key, rawOffset := range offsets {
				searchOffset := int(rawOffset.(float64))

				messages, ok := logs[key]
				if !ok {
					continue
				}

				for _, entry := range messages {
					if entry[0] >= searchOffset {
						returnedMessages[key] = append(returnedMessages[key], entry)
					}
				}
			}

		response["msgs"] = returnedMessages


		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		response := make(map[string]any)
		response["type"] = "commit_offsets_ok"

		client := msg.Src
		offsets := body["offsets"].(map[string]any)

		mu.Lock()
		defer mu.Unlock()

		if committed[client] == nil {
			committed[client] = make(map[string]int)
		}

		for key, raw := range offsets {
			offset := int(raw.(float64))
			if cur, ok := committed[client][key]; !ok || offset > cur {
				committed[client][key] = offset
			}
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		client := msg.Src
		rawKeys := body["keys"].([]any)

		offsets := make(map[string]int)

		mu.Lock()
		defer mu.Unlock()

		for _, rk := range rawKeys {
			key := rk.(string)
			if committed[client] != nil {
				if off, ok := committed[client][key]; ok {
					offsets[key] = off
				}
			}
		}
		
		response := make(map[string]any)
		response["offsets"] = offsets

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
    	log.Fatal(err)
	}
}