package client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	// "github.com/segmentio/kafka-go/sasl/plain"
)

const RootCACert = `-----BEGIN CERTIFICATE-----
MIICuDCCAmqgAwIBAgIURKmZE5o9LPqEQpU6yahiP+TLwpAwBQYDK2VwMIHHMQsw
CQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZy
YW5jaXNjbzEYMBYGA1UECgwPTWlzdGVyIFdlYmhvb2tzMRQwEgYDVQQLDAtFbmdp
bmVlcmluZzErMCkGA1UEAwwiS2Fma2EgQnJva2VyIENlcnRpZmljYXRlIEF1dGhv
cml0eTEuMCwGCSqGSIb3DQEJARYfZW5naW5lZXJpbmdAbWlzdGVyLXdlYmhvb2tz
LmNvbTAeFw0yNTA1MjIwNDA0NTZaFw0zNTA1MjAwNDA0NTZaMIHHMQswCQYDVQQG
EwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNj
bzEYMBYGA1UECgwPTWlzdGVyIFdlYmhvb2tzMRQwEgYDVQQLDAtFbmdpbmVlcmlu
ZzErMCkGA1UEAwwiS2Fma2EgQnJva2VyIENlcnRpZmljYXRlIEF1dGhvcml0eTEu
MCwGCSqGSIb3DQEJARYfZW5naW5lZXJpbmdAbWlzdGVyLXdlYmhvb2tzLmNvbTAq
MAUGAytlcAMhAE4/M7Qj1+KNtqGdGF7DgAtO+elzPGDHlyCLz1VCvwi+o2YwZDAd
BgNVHQ4EFgQUVVOr9w+0L3obSHwAx/3DKG+iKOMwHwYDVR0jBBgwFoAUVVOr9w+0
L3obSHwAx/3DKG+iKOMwEgYDVR0TAQH/BAgwBgEB/wIBATAOBgNVHQ8BAf8EBAMC
AQYwBQYDK2VwA0EAZlSOhxGZrIK/gUwB6tOKK3S0gvD7a+SoEEkAYVF44AnwvMe0
5qzICSe+0sFaqLT0CNf2JQo/PSK06e9Lb7zNCw==
-----END CERTIFICATE-----`

var validate = validator.New(validator.WithRequiredStructEnabled())

// A ConnectionProfile contains everything needed to start a Consumer for webhook events.
type ConnectionProfile struct {
	consumerName string
	auth         sasl.Mechanism
	tls          *tls.Config
	broker       net.Addr
}

type Hostname struct {
	str string
}

func (*Hostname) Network() string {
	return "tcp"
}

func (h *Hostname) String() string {
	return h.str
}

// LoadConnectionProfile reads a ConnectionProfile from path.
//
// The caller must provide a type hint when calling LoadConnectionProfile. For example:
//
//	client.LoadConnectionProfile[map[string]any](profilePath)
//
// or
//
//	client.LoadConnectionProfile[HelloWorld](profilePath)
//
// If a type other than map[string]any is used, decoding will procede according to the
// [encoding/json] rules.
func LoadConnectionProfile(path string) (*ConnectionProfile, error) {
	p := struct {
		ConsumerName string `json:"consumer_name"`
		Auth         struct {
			Mechanism string `json:"mechanism"`
			Secret    string `json:"secret"`
		} `json:"auth"`
		Kafka struct {
			Bootstrap string `json:"bootstrap"`
		}
	}{}

	f, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	data, err := io.ReadAll(f)

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("json decoding error: %w", err)
	}

	var mechanism sasl.Mechanism

	switch p.Auth.Mechanism {
	case "plain":
		mechanism = plain.Mechanism{
			Username: p.ConsumerName,
			Password: p.Auth.Secret,
		}
	default:
		return nil, fmt.Errorf("'%s' is not a supported auth mechanism", p.Auth.Mechanism)
	}

	rootCAs := x509.NewCertPool()

	ok := rootCAs.AppendCertsFromPEM(
		[]byte(RootCACert),
	)

	if !ok {
		return nil, fmt.Errorf("error adding RootCA cert")
	}

	candidate := ConnectionProfile{
		consumerName: p.ConsumerName,
		auth:         mechanism,
		tls: &tls.Config{
			RootCAs: rootCAs,
		},
		broker: &Hostname{p.Kafka.Bootstrap},
	}

	if err = validate.Struct(candidate); err != nil {
		return nil, err
	}

	return &candidate, nil
}
