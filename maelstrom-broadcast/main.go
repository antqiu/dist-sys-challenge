package main

import (
    "encoding/json"
    "log"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	topology := make(map[string][]string)

	seen_messages_arr := make([]int, 0)
	seen_messages := make(map[int]bool) // equivalent of a set

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		n_id := n.ID() // node id: string - this must be in the handler because it guarantees that it executes after init

		
		response := make(map[string]any)
		response["type"] = "broadcast_ok"

		f, ok := body["message"].(float64) // assert that it is at least type float
		
		if ok {
			in := int(f) // if float, convert to int
			_, seen_ok := seen_messages[in]
			if !seen_ok {
				// append, broadcast
				seen_messages_arr = append(seen_messages_arr, in)
				_, topology_ok := topology[n_id]
				if topology_ok {
					for _, neighborString := range topology[n_id] {
						n.Send(neighborString, map[string]any{
							"type":    "broadcast",
							"message": in,
							"broadcasted" : "yes",
						})
					}
				}
				seen_messages[in] = true
			}
		}

		_, broadcasted := body["broadcasted"]
		if !broadcasted {
			return n.Reply(msg, response)
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := make(map[string]any)

		// Update the message type to return back.
		response["type"] = "read_ok"
		response["messages"] = seen_messages_arr

		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := make(map[string]any)

		// Update the message type to return back.
		response["type"] = "topology_ok"

		rawTopo, ok := body["topology"].(map[string]any)
		if !ok {
			return n.Reply(msg, response)
		}

		topology = make(map[string][]string)

		for node, neighborsAny := range rawTopo {
			neighborsSlice, ok := neighborsAny.([]any)
			if !ok {
				continue
			}

			neighbors := make([]string, 0, len(neighborsSlice))
			for _, n := range neighborsSlice {
				if s, ok := n.(string); ok {
					neighbors = append(neighbors, s)
				}
			}

			topology[node] = neighbors
		}

		log.Printf("[%s] parsed topology: %#v", n.ID(), topology)
		
		// Echo the original message back with the updated message type.
		return n.Reply(msg, response)
	})


	if err := n.Run(); err != nil {
    	log.Fatal(err)
	}

}
