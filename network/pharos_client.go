package network

import (
	"fmt"
	"github.com/op/go-logging"
	"net/http"
	"net/http/cookiejar"
)

type PharosClient struct {
	hostUrl      string
	apiVersion   string
	apiUser      string
	apiKey       string
	httpClient   *http.Client
	transport    *http.Transport
	logger       *logging.Logger
	institutions map[string]string
}

// Creates a new pharos client. Param hostUrl should come from
// the config.json file.
func NewPharosClient(hostUrl, apiVersion, apiUser, apiKey string, logger *logging.Logger) (*PharosClient, error) {
	// see security warning on nil PublicSuffixList here:
	// http://gotour.golang.org/src/pkg/net/http/cookiejar/jar.go?s=1011:1492#L24
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for HTTP client: %v", err)
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 8,
		DisableKeepAlives:   false,
	}
	httpClient := &http.Client{Jar: cookieJar, Transport: transport}
	return &PharosClient{hostUrl, apiVersion, apiUser, apiKey, httpClient, transport, logger, nil}, nil
}
