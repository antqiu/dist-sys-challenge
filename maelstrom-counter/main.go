package main

import (
    "encoding/json"
    "log"
	"time"
	"context"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n) // sequentially consistent counter
	node_ids := []string{}

	n.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		raw := body["node_ids"].([]any)
		node_ids = make([]string, len(raw))
		for i, id := range raw {
			node_ids[i] = id.(string)
		}

		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "add_ok"
		f, _ := body["delta"].(float64) 
		value := int(f)

		d, e := kv.Read(context.Background(), n.ID())
		if e != nil || d == nil {
			dict := make(map[string]int)
			dict[n.ID()] = value
			_ = kv.Write(context.Background(), n.ID(), dict)
		} else {
			raw := d.(map[string]any)

			localDict := make(map[string]int)
			for k, v := range raw {
				localDict[k] = int(v.(float64))
			}

			localDict[n.ID()] += value
			_ = kv.Write(context.Background(), n.ID(), localDict)
		}

		response := make(map[string]any)
		response["type"] = "add_ok"

		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		d, err := kv.Read(context.Background(), n.ID())

		sum := 0
		if err == nil && d != nil {
			raw := d.(map[string]any)
			for _, v := range raw {
				sum += int(v.(float64))
			}
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": sum,
		})
	})


	n.Handle("merge_vector", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		remote := make(map[string]int)
		rawRemote := body["vector"].(map[string]any)
		for k, v := range rawRemote {
			remote[k] = int(v.(float64))
		}

		local := make(map[string]int)
		d, err := kv.Read(context.Background(), n.ID())
		if err == nil && d != nil {
			bytes, _ := json.Marshal(d)
			json.Unmarshal(bytes, &local)
		}
		
		if d != nil {
			rawLocal := d.(map[string]any)
			for k, v := range rawLocal {
				local[k] = int(v.(float64))
			}
		}

		merged := merge(local, remote)

		kv.Write(context.Background(), n.ID(), merged)

		response:= make(map[string]any)
		response["type"] = "merge_vector_ok"

		return n.Reply(msg, response)
	})

	n.Handle("merge_vector_ok", func(msg maelstrom.Message) error {
		return nil
	})

	go func() {
		ticker := time.NewTicker(1000 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			d, _ := kv.Read(context.Background()``, n.ID())
			local := make(map[string]int)
			if d != nil {
				raw := d.(map[string]any)
				for k, v := range raw {
					local[k] = int(v.(float64))
				}
			}

			for _, otherNode := range node_ids { 
				// loop through other nodes, update their vectors with merged vector including the current node's vector
				if otherNode == n.ID() {
					continue 
				}

				n.Send(otherNode, map[string]any{
					"type":   "merge_vector",
					"vector": local,
				})
			}
		}
	}()

	
	if err := n.Run(); err != nil {
    	log.Fatal(err)
	}
}

func merge(local, remote map[string]int) map[string]int {
    merged := make(map[string]int)

    for node, val := range local {
        merged[node] = val
    }

    for node, val := range remote {
        if existing, ok := merged[node]; !ok || val > existing {
            merged[node] = val
        }
    }

    return merged
}
