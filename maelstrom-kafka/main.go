package main

import (
    "encoding/json"
    "log"
	// "time"
	// "math/rand"
	"sync"
	"strconv"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	logs := make(map[string][][]int)
	
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
		keyId, _ := strconv.Atoi(key[1:]) // key, increment most recent key by 1 every time
		
		mu.Lock()
		defer mu.Unlock()
		
		nextOffset := keyId * 1000

		if (logs[key] != nil) {
			// mu.Lock()
			// mostRecentEntry := logs[keyId][-1]
			// mu.Unlock()

			// mostRecentOffset := mostRecentEntry[0] // last offset 
			nextOffset = len(logs[key]) + 1
		 }

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

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys, _ := body["keys"].([]string)
		
		// Update the message type to return back.
		response := make(map[string]any)
		response["type"] = "list_committed_offsets_ok"
		
		offsets := make(map[string]int)
		for _, key := range keys {
			if (logs[key] != nil) {
				keyId, _ := strconv.Atoi(key[1:]) // key, increment most recent key by 1 every time
				nextOffset := keyId * 1000
				offsets[key] = nextOffset
			}
		}
		response["offsets"] = offsets	

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
    	log.Fatal(err)
	}
}