package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
	"encoding/json"
	"log"
	"strconv"
	"regexp"
	"net/http"
	"bytes"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
)

// MongoDB connection details 

const mongouri = "mongodb://localhost:27017/api"
const mongodatabase = "myapp_development"
const mongocollection = "sources"

// MutateCommand represents a command in our DSL
type MutateCommand struct {
	Action    string
	Arguments []string
}

func prettyPrintJSON(jsonStr string) {
	var data interface{}
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		fmt.Println("Error unmarshaling JSON:", err)
		return
	}

	prettyJSON, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}

	fmt.Println(string(prettyJSON))
}
func TraverseMap(data map[string]interface{}, path string) (interface{}, bool) {
	keys := strings.Split(path, ".")
	var current interface{} = data

	arrayIndexPattern := regexp.MustCompile(`^(\w+)\[(\d+)\]$`)

	for _, key := range keys {
		// Check for array index notation
		if matches := arrayIndexPattern.FindStringSubmatch(key); matches != nil {
			key = matches[1]
			index, err := strconv.Atoi(matches[2])
			if err != nil {
				return nil, false
			}

			// Check if the current level is a map
			if m, ok := current.(map[string]interface{}); ok {
				// Try to get the value for the current key
				if val, exists := m[key]; exists { 
					// Check if the value is an array
					if arr, ok := val.([]interface{}); ok {
						if index >= 0 && index < len(arr) {
							current = arr[index]
						} else {
							// Index out of bounds
							return nil, false
						}
					} else {
						// Value is not an array
						return nil, false
					}
				} else {
					// Key does not exist
					return nil, false
				}
			} else {
				// Current level is not a map
				return nil, false
			}
		} else {
			// Check if the current level is a map
			if m, ok := current.(map[string]interface{}); ok {
				// Try to get the value for the current key
				if val, exists := m[key]; exists {
					current = val
				} else {
					// Key does not exist
					return nil, false
				}
			} else {
				// Current level is not a map
				return nil, false
			}
		}
	}
	// Successfully traversed the path
	return current, true
}

func ApplyMutations(result map[string]interface{}, commands []MutateCommand, topic string) {
	
	result["eventSource"] = topic
	for _, cmd := range commands {
		switch cmd.Action {
		case "rename":
			if len(cmd.Arguments) == 2 {
				oldName := cmd.Arguments[0]
				newName := cmd.Arguments[1]
				if val, ok := result[oldName]; ok {
					result[newName] = val
					delete(result, oldName)
				}
			}
		case "update":
			if len(cmd.Arguments) == 2 {
				fieldName := cmd.Arguments[0]
				newValue := cmd.Arguments[1]
				result[fieldName] = newValue
			}

		case "dig":
			if len(cmd.Arguments) == 2 {
				fieldName := cmd.Arguments[0]
				newField := cmd.Arguments[1]
				path := fieldName
				value, ok := TraverseMap(result, path)
				if ok {
					//fmt.Printf("Value at path '%s': %v\n", path, value)
					result[newField] = value
				} else {
					//fmt.Printf("Path '%s' not found\n", path)
				}
			}
		case "deleteKey":
			if len(cmd.Arguments) == 1 {
				fieldName := cmd.Arguments[0]
				delete(result, fieldName)
			}
		}

		

	}
}

// ParseDSL parses the DSL string into a slice of MutateCommand
func ParseDSL(dsl string) ([]MutateCommand, error) {
	lines := strings.Split(dsl, "\n")
	
	var commands []MutateCommand
	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) > 1 {
			cmd := MutateCommand{
				Action:    parts[0],
				Arguments: parts[1:],
			}
			commands = append(commands, cmd)
		}
	}
	return commands, nil
}

func main() {
	// Define command-line flags
	var (
		broker = flag.String("broker", "192.168.1.201:9092", "The Kafka broker address")
		group  = flag.String("group", "myGroup", "The Kafka consumer group")
		topic  = flag.String("topic", "myTopic", "The Kafka topic to subscribe to")
	)

	// Parse the flags
	flag.Parse()
	fmt.Println("\n\x1b[32mStarting Subscriber Processor.....\x1b[0m\n")
	fmt.Println("\x1b[32mStarting mongo connection.....\x1b[0m\n")

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
   	opts := options.Client().ApplyURI(mongouri).SetServerAPIOptions(serverAPI)
   	// Create a new client and connect to the server
   	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
	// Send a ping to confirm a successful connection
	var ping bson.M
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{Key: "ping", Value: 1}}).Decode(&ping); err != nil { 
		panic(err)
	}
	fmt.Println("\x1b[32mPinged your deployment. You successfully connected to MongoDB!\x1b[0m\n ")
	fmt.Println("\x1b[33mThe subscribed topic is \x1b[0m" , *topic)


	
	// Create a new consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": *broker,
		"group.id":          *group,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	// Subscribe to the topic
	err = c.SubscribeTopics([]string{*topic}, nil)
	if err != nil {
		panic(err)
	}

	var result map[string]interface{} 

	// Consume messages
	fmt.Println("\x1b[32mWaiting for messages .....\x1b[0m\n")
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("\x1b[32mReceived message: %s: \x1b[0m\n", msg.TopicPartition)
			fmt.Printf("%s", string(msg.Value))

			coll1 := client.Database(mongodatabase).Collection(mongocollection)
			filter := bson.D{{ Key: "source", Value: topic }}
			fmt.Println("")
			row := bson.M{}
			opts1 := options.FindOne()
			err2 := coll1.FindOne(context.TODO(), filter, opts1).Decode(& row)

			if err2 != nil {
				fmt.Println("DB error is ",err2)
				return
			}

			//fmt.Println("The transformer code is .......\n\n")
			//fmt.Println(row["transformer"])

			transformer, ok := row["transformer"]
			if !ok {
				fmt.Println("Name field not found in BSON map")
				return
			}


			jsonData := string(msg.Value)
			result = make(map[string]interface{})
			err := json.Unmarshal([]byte(jsonData), &result)
			if err != nil {
				log.Fatal(err)
			}

			transformer1, ok := transformer.(string)
			if !ok {
				fmt.Println("Name field is not a string")
				return
			}

			commands, err := ParseDSL(transformer1)
			if err != nil {
				fmt.Println("Error parsing DSL:", err)
				return
			}
		
			ApplyMutations(result, commands, *topic )
		
			//fmt.Println("Mutated Log Entry:", result)
			j,_ := json.Marshal(result)
			//fmt.Println("Mutated Log JSON:", string(j))

			event := make(map[string]map[string]interface{})
			var result map[string]interface{}

			err1 := json.Unmarshal(j, &result)
			if err1 != nil {
				fmt.Println("Error:", err1)
				return
			}
			event["event"] = result

			jsonString, err := json.Marshal(event)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			//fmt.Println("Final Mutated Log JSON:", string(jsonString))

			url := "http://192.168.1.201:3000/events.json"
			req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonString))
			if err != nil {
				fmt.Println("Error creating request:", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
		
			// Send the request using an HTTP client
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("Error sending request:", err)
				return
			}
			defer resp.Body.Close()
			fmt.Println("\x1b[32mResponse status:\x1b[0m", resp.Status)
			// Read the response body
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("Error reading response body:", err)
				return
			}

			// Convert the response body to a string
			bodyString := string(body)
			//fmt.Println("Response Body:", bodyString)
			prettyPrintJSON(bodyString)
			fmt.Println("\n\x1b[33m**************************************************************************\x1b[0m\n")
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
		_, err3 := c.CommitMessage(msg)
		if err3 != nil {
			fmt.Printf("Failed to commit message: %v\n", err3)
		}
	}
	// Close the consumer when done
	c.Close()
}
