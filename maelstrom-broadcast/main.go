package main

import (
    "encoding/json"
    "log"
	"time"
	// "math/rand"
	"sync"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type delivery struct {
	dest string
	message int
}

var mu sync.Mutex

func main() {
	n := maelstrom.NewNode()
	topology := make(map[string][]string)
	initialized := false

	seen_messages_arr := make([]int, 0)
	seen_messages := make(map[int]bool) // equivalent of a set
	// pending := make([]int, 0)

	pendingDeliveries := make(map[delivery]bool)  // not yet sent
	unconfirmedDeliveries := make(map[delivery]bool) // sent but not ACKed

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		response := make(map[string]any)
		response["type"] = "broadcast_ok"

		f, _ := body["message"].(float64) // assert that it is at least type float
		in := int(f) // if float, convert to int

		mu.Lock()
		_, seen_ok := seen_messages[in]
		if !seen_ok {
			seen_messages_arr = append(seen_messages_arr, in)
			seen_messages[in] = true
			
			for _, neighbor := range topology[n.ID()] {
				d := delivery{dest: neighbor, message: in}
				pendingDeliveries[d] = true
			}
		}
		mu.Unlock()

	
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
		mu.Lock()
		msgs := make([]int, len(seen_messages_arr))
		copy(msgs, seen_messages_arr)
		mu.Unlock()

		response["messages"] = msgs


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

	n.Handle("deliver", func(msg maelstrom.Message) error {
		// is an array from gossiping, run throughh all of different values and gossip those
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		receiving_arr, ok := body["message"].([]any)
		processed_arr := make([]int, 0)

		// if (len(receiving_arr) != len(seen_messages_arr) && ok) {

		if (ok) {
			mu.Lock()
			for _, v := range receiving_arr {
				value := int(v.(float64))
				_, ok = seen_messages[value]
				if !ok {
					seen_messages[value] = true 
					seen_messages_arr = append(seen_messages_arr, value)
					// pending = append(pending, value)
					processed_arr = append(processed_arr, value)

					// enqueue forwarding to neighbors
					for _, neighbor := range topology[n.ID()] {
						if neighbor == msg.Src {
							continue
						}
						d := delivery{dest: neighbor, message: value}
						pendingDeliveries[d] = true
					}
				}
			}
			mu.Unlock()
		}

		response := make(map[string]any)
		response["type"] = "deliver_ok"
		response["received_message"] = processed_arr

		return n.Reply(msg, response)
	})

	n.Handle("deliver_ok", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		rawArr, ok := body["received_message"].([]any)
		if !ok {
			return nil
		}

		mu.Lock()
		for _, v := range rawArr {
			val := int(v.(float64))
			d := delivery{dest: msg.Src, message: val}
			delete(unconfirmedDeliveries, d)
		}
		mu.Unlock()

		return nil
	})

	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			if !initialized {
				continue
			}
			gossip(n, topology, pendingDeliveries, unconfirmedDeliveries)
		}
	}()

	if err := n.Run(); err != nil {
    	log.Fatal(err)
	}

}

func gossip(n *maelstrom.Node, topology map[string][]string, pending map[delivery]bool, unconfirmed map[delivery]bool) {
	batches := make(map[string][]int)

	mu.Lock()
	for d := range pending {
		batches[d.dest] = append(batches[d.dest], d.message)
	}
	mu.Unlock()

	for dest, msgs := range batches {
		if len(msgs) == 0 {
			continue
		}

		n.Send(dest, map[string]any{
			"type":    "deliver",
			"message": msgs,
		})

		mu.Lock()
		for _, m := range msgs {
			d := delivery{dest: dest, message: m}
			delete(pending, d)
			unconfirmed[d] = true
		}
		mu.Unlock()
	}
}