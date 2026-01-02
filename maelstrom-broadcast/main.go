package main

import (
    "encoding/json"
    "log"
	"time"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


func main() {
	n := maelstrom.NewNode()
	topology := make(map[string][]string)
	initialized := false

	seen_messages_arr := make([]int, 0)
	seen_messages := make(map[int]bool) // equivalent of a set

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// n_id := n.ID() // node id: string - this must be in the handler because it guarantees that it executes after init
		
		response := make(map[string]any)
		response["type"] = "broadcast_ok"

		// f, ok := body["message"].(float64) // assert that it is at least type float
		
		// if ok {
		// 	in := int(f) // if float, convert to int
		// 	_, seen_ok := seen_messages[in]
		// 	if !seen_ok {
		// 		// append, broadcast
		// 		seen_messages_arr = append(seen_messages_arr, in)
		// 		_, topology_ok := topology[n_id]
		// 		if topology_ok {
		// 			for _, neighborString := range topology[n_id] {
		// 				n.Send(neighborString, map[string]any{
		// 					"type":    "broadcast",
		// 					"message": in,
		// 				})
		// 			}
		// 		}
		// 		seen_messages[in] = true
		// 	}
		// }

		_, ok := body["is_array"]
		if ok { 
			// is an array from gossiping, run throughh all of different values and gossip those
			receiving_arr, ok := body["message"].([]any)

			if (len(receiving_arr) != len(seen_messages_arr) && ok) {
				for _, v := range receiving_arr {
					value := int(v.(float64))
					_, ok = seen_messages[value]
					if !ok {
						seen_messages[value] = true 
						seen_messages_arr = append(seen_messages_arr, value)
					}
				}
			}
		} else {
			// single message broadcast
			f, _ := body["message"].(float64) // assert that it is at least type float
			in := int(f) // if float, convert to int

			_, seen_ok := seen_messages[in]
			if !seen_ok {
				seen_messages_arr = append(seen_messages_arr, in)
				seen_messages[in] = true
			}
		}
	
		if msg.Src[0] == 'c' {
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

		initialized = true

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


	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			if !initialized {
				continue
			}
			gossip(n, topology, seen_messages_arr)
		}
	}()


	if err := n.Run(); err != nil {
    	log.Fatal(err)
	}

}

func gossip(n *maelstrom.Node, topology map[string][]string, seen []int) {
		id := n.ID()

		for _, neighbor := range topology[id] {
			n.Send(neighbor, map[string]any{
				"type":    "broadcast",
				"message": seen,
				"is_array": "yes",
			})
		}
}