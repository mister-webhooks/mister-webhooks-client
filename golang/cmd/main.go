// Package main contains an example client for Mister Webhooks.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/mister-webhooks/mister-webhooks-client/client"
)

func main() {
	//
	// Parse arguments from the command line
	//
	flag.Parse()

	topicName := flag.Arg(0)
	profilePath := flag.Arg(1)

	if topicName == "" || profilePath == "" {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "main: [options] <topic> <filename>")
		fmt.Fprintln(os.Stderr, "\truns a console consumer on the given topic using the provided connection profile")
		os.Exit(64) // EX_USAGE
	}

	// Load the Mister Webhooks connection profile from a file on the filesystem.
	profile, err := client.LoadConnectionProfile(profilePath)

	if err != nil {
		log.Fatal(err)
	}

	// Create a consumer that reads generic nested dictionaries from `topicName`.
	consumer, err := client.NewConsumer(
		profile,
		client.DeclareWebhookTopic[map[string]any](topicName),
	)

	if err != nil {
		log.Fatal(err)
	}

	// Loop endlessly (or until a network error) reading nested dictionaries and dumping them to stdout. In a real consumer,
	// this is where you'd place your custom handling code. Replace `map[string]any` with a type that has `json` struct tags
	// and you'll get automatical deserialization of event payloads into an instance of that type. When your handler returns
	// an error, consumer.Consume() will cleanly shut down and then return that error.
	err = consumer.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		log.Println(spew.Sdump(event))
		return nil
	})

	if err != nil {
		log.Fatalf("error: %s", err)
	}
}
