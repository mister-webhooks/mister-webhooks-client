package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/hamba/avro/v2"
	"github.com/mister-webhooks/mister-webhooks-client/client"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const exampleCA = `-----BEGIN CERTIFICATE-----
MIIDTzCCArCgAwIBAgIBATAKBggqhkjOPQQDAjCBuTEbMBkGA1UEAwwSTWlzdGVy
IFdlYmhvb2tzIENBMRgwFgYDVQQKDA9NaXN0ZXIgV2ViaG9va3MxFDASBgNVBAsM
C0VuZ2luZWVyaW5nMQswCQYDVQQIDAJDQTELMAkGA1UEBhMCVVMxFjAUBgNVBAcM
DVNhbiBGcmFuY2lzY28xODA2BgkqhkiG9w0BCQEWKWNlcnRpZmljYXRlLWF1dGhv
cml0eUBtaXN0ZXItd2ViaG9va3MuY29tMB4XDTI1MDUxOTE5MzgxNFoXDTM1MDUx
NzE5MzgxNFowgbkxGzAZBgNVBAMMEk1pc3RlciBXZWJob29rcyBDQTEYMBYGA1UE
CgwPTWlzdGVyIFdlYmhvb2tzMRQwEgYDVQQLDAtFbmdpbmVlcmluZzELMAkGA1UE
CAwCQ0ExCzAJBgNVBAYTAlVTMRYwFAYDVQQHDA1TYW4gRnJhbmNpc2NvMTgwNgYJ
KoZIhvcNAQkBFiljZXJ0aWZpY2F0ZS1hdXRob3JpdHlAbWlzdGVyLXdlYmhvb2tz
LmNvbTCBmzAQBgcqhkjOPQIBBgUrgQQAIwOBhgAEADBcixtsoeErtv6sMTL4XYRC
VAz4cXA68ZAIOQi2hTsMyFlOEOnohByH8Q85WreWGT+6jQU9f/CJaB7GMygFXMrY
ACURCtCKzQLX5szCLZJ1ek57bCIRSyDdENsQoVgDrliubQNQ/5AZH9tSyjWXLlmE
ENHi/ef+LDj9ZDoMw9TFGomoo2QwYjAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB
/wQEAwICpDAgBgNVHSUBAf8EFjAUBggrBgEFBQcDAgYIKwYBBQUHAwEwHQYDVR0O
BBYEFO6W7n09kAegJgvtOHJjX0Gxl4UuMAoGCCqGSM49BAMCA4GMADCBiAJCAW+v
YzRMnnqB7lHI+AinUviaIiMiQwCuoYJgZtn7h7Ymz3hQcBY6cLZ+BOwCq1OMYHQX
ocPBOKNkbKx/+2EwLuiMAkIBUt1DYTfSu5NpUddGcJwE7ICNLBthTMxBpdzRPkoV
1RbvJ87VRibTpsQAYY5x8TwEN2Fa6l47Lk3J0b40lEPSlbg=
-----END CERTIFICATE-----`

const connectionProfileJSON = `{
	"consumer_name": "%s",
	"auth": {
	  "mechanism": "%s",
		"secret": "%s"
	},
	"kafka": {
		"servers": ["%s"],
    "certificate_authorities": [%s]
	}
}`

func TestLoadConnectionProfile(t *testing.T) {
	tmp, err := os.CreateTemp("", "connection-profile-*.json")

	require.NoError(t, err)

	defer os.Remove(tmp.Name())

	jsonCA, err := json.Marshal(exampleCA)

	require.NoError(t, err)

	profileData := fmt.Sprintf(
		connectionProfileJSON,
		"integration.testing",
		"plain",
		"foo-bar-baz",
		"127.0.0.1:9092",
		jsonCA,
	)

	_, err = tmp.Write([]byte(profileData))

	require.NoError(t, err)

	_, err = client.LoadConnectionProfile(tmp.Name())

	require.NoError(t, err)
}

func TestConsumerErrorOnMissingHeader(t *testing.T) {
	missingHeader, err := client.NewScriptedConsumer(
		client.DeclareWebhookTopic[map[string]any]("topic.good"),
		[][]*kafka.Message{
			{
				&kafka.Message{
					Topic:     "topic.good",
					Partition: 0,
					Offset:    0,
					Key:       []byte("cyka"),
					Value:     []byte("blyat"),
					Headers:   []kafka.Header{},
					Time:      time.Time{},
				},
			},
		})

	require.NoError(t, err)

	err = missingHeader.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		assert.Fail(t, "decoding errors shouldn't trigger the event handler")
		return nil
	})

	assert.ErrorContains(t, err, "malformed Kafka message, 'envelope' header is missing")
}

func TestConsumerErrorOnBadAvro(t *testing.T) {
	missingHeader, err := client.NewScriptedConsumer(
		client.DeclareWebhookTopic[map[string]any]("topic.good"),
		[][]*kafka.Message{
			{
				&kafka.Message{
					Topic:     "topic.good",
					Partition: 0,
					Offset:    0,
					Key:       []byte("cyka"),
					Value:     []byte("blyat"),
					Headers: []kafka.Header{
						{
							Key:   "envelope",
							Value: []byte{0x80},
						},
					},
					Time: time.Time{},
				},
			},
		})

	require.NoError(t, err)

	err = missingHeader.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		assert.Fail(t, "decoding errors shouldn't trigger the event handler")
		return nil
	})

	assert.ErrorContains(t, err, "Method: avro: decode enum symbol: unknown enum symbol")
}

func TestConsumerIgnoresUnsupportedSystemEvent(t *testing.T) {
	systemEvent, err := client.NewScriptedConsumer(
		client.DeclareWebhookTopic[map[string]any]("topic.good"),
		[][]*kafka.Message{
			{
				&kafka.Message{
					Topic:     "topic.good",
					Partition: 0,
					Offset:    1,
					Key:       []byte("foobar"),
					Value:     []byte("also foobar"),
					Headers: []kafka.Header{
						{
							Key:   "envelope",
							Value: []byte{0x00},
						},
					},
					Time: time.Time{},
				},
			},
		})

	require.NoError(t, err)

	err = systemEvent.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		assert.Fail(t, "system events shouldn't trigger the event handler")
		return nil
	})

	assert.NoError(t, err)
}

func TestConsumerIgnoresUnsupportedDataEvent(t *testing.T) {
	systemEvent, err := client.NewScriptedConsumer(
		client.DeclareWebhookTopic[map[string]any]("topic.good"),
		[][]*kafka.Message{
			{
				&kafka.Message{
					Topic:     "topic.good",
					Partition: 0,
					Offset:    1,
					Key:       []byte("foobar"),
					Value:     []byte("also foobar"),
					Headers: []kafka.Header{
						{
							Key:   "envelope",
							Value: []byte{0xff},
						},
					},
					Time: time.Time{},
				},
			},
		})

	require.NoError(t, err)

	err = systemEvent.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		assert.Fail(t, "system events shouldn't trigger the event handler")
		return nil
	})

	assert.ErrorContains(t, err, "envelope type 0xff is unsupported, please upgrade mister-webhooks-client")
}

func TestConsumerProcessesWebhookValidJSON(t *testing.T) {
	data, err := avro.Marshal(client.KafkaMessageEnvelopeV1Schema, &client.KafkaMessageEnvelopeV1{
		Method:   client.HTTPMethodPOST,
		Headers:  map[string][]string{"hello": {"world"}},
		Payload:  []byte(`{"i": "live"}`),
		Encoding: "JSON",
	})

	require.NoError(t, err)

	systemEvent, err := client.NewScriptedConsumer(
		client.DeclareWebhookTopic[map[string]any]("topic.good"),
		[][]*kafka.Message{
			{
				&kafka.Message{
					Topic:     "topic.good",
					Partition: 0,
					Offset:    1,
					Key:       []byte("foobar"),
					Value:     data,
					Headers: []kafka.Header{
						{
							Key:   "envelope",
							Value: []byte{0x80},
						},
					},
					Time: time.Time{},
				},
			},
		})

	require.NoError(t, err)

	err = systemEvent.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		assert.Equal(t, client.HTTPMethodPOST, event.Method)
		assert.Equal(t, []string{"world"}, event.Headers["hello"])
		assert.Equal(t, "live", (*event.Payload)["i"])
		return nil
	})

	assert.NoError(t, err)
}

func TestConsumerProcessesWebhookInvalidJSON(t *testing.T) {
	data, err := avro.Marshal(client.KafkaMessageEnvelopeV1Schema, &client.KafkaMessageEnvelopeV1{
		Method:   client.HTTPMethodPOST,
		Headers:  map[string][]string{"hello": {"world"}},
		Payload:  []byte(`{"i": donotlive}`),
		Encoding: "JSON",
	})

	require.NoError(t, err)

	systemEvent, err := client.NewScriptedConsumer(
		client.DeclareWebhookTopic[map[string]any]("topic.good"),
		[][]*kafka.Message{
			{
				&kafka.Message{
					Topic:     "topic.good",
					Partition: 0,
					Offset:    1,
					Key:       []byte("foobar"),
					Value:     data,
					Headers: []kafka.Header{
						{
							Key:   "envelope",
							Value: []byte{0x80},
						},
					},
					Time: time.Time{},
				},
			},
		})

	require.NoError(t, err)

	err = systemEvent.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		assert.Fail(t, "decoding errors shouldn't trigger the event handler")
		return nil
	})

	assert.IsType(t, err, &json.SyntaxError{})
}

func TestConsumerProcessesWebhookValidCBOR(t *testing.T) {
	payload, err := cbor.Marshal(&struct {
		Foo string `cbor:"foo"`
	}{Foo: "seven"})

	require.NoError(t, err)

	data, err := avro.Marshal(client.KafkaMessageEnvelopeV1Schema, &client.KafkaMessageEnvelopeV1{
		Method:   client.HTTPMethodPOST,
		Headers:  map[string][]string{"hello": {"world"}},
		Payload:  payload,
		Encoding: "CBOR",
	})

	require.NoError(t, err)

	systemEvent, err := client.NewScriptedConsumer(
		client.DeclareWebhookTopic[map[string]any]("topic.good"),
		[][]*kafka.Message{
			{
				&kafka.Message{
					Topic:     "topic.good",
					Partition: 0,
					Offset:    1,
					Key:       []byte("foobar"),
					Value:     data,
					Headers: []kafka.Header{
						{
							Key:   "envelope",
							Value: []byte{0x80},
						},
					},
					Time: time.Time{},
				},
			},
		})

	require.NoError(t, err)

	err = systemEvent.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		assert.Equal(t, client.HTTPMethodPOST, event.Method)
		assert.Equal(t, []string{"world"}, event.Headers["hello"])
		assert.Equal(t, "seven", (*event.Payload)["foo"])
		return nil
	})

	assert.NoError(t, err)
}

func TestConsumerProcessesWebhookInvalidCBOR(t *testing.T) {
	data, err := avro.Marshal(client.KafkaMessageEnvelopeV1Schema, &client.KafkaMessageEnvelopeV1{
		Method:   client.HTTPMethodPOST,
		Headers:  map[string][]string{"hello": {"world"}},
		Payload:  []byte("lolnope"),
		Encoding: "CBOR",
	})

	require.NoError(t, err)

	systemEvent, err := client.NewScriptedConsumer(
		client.DeclareWebhookTopic[map[string]any]("topic.good"),
		[][]*kafka.Message{
			{
				&kafka.Message{
					Topic:     "topic.good",
					Partition: 0,
					Offset:    1,
					Key:       []byte("foobar"),
					Value:     data,
					Headers: []kafka.Header{
						{
							Key:   "envelope",
							Value: []byte{0x80},
						},
					},
					Time: time.Time{},
				},
			},
		})

	require.NoError(t, err)

	err = systemEvent.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		assert.Fail(t, "decoding errors shouldn't trigger the event handler")
		return nil
	})

	assert.Error(t, err)
}
