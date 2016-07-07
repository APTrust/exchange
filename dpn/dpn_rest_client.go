package dpn

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Don't log error messages longer than this
const MAX_ERR_MSG_SIZE = 2048

// DPNRestClient is a client for the DPN REST API.
type DPNRestClient struct {
	HostUrl      string
	APIVersion   string
	APIKey       string
	Node         string
	dpnConfig    *models.DPNConfig
	httpClient   *http.Client
	transport    *http.Transport
}

type NodeListResult struct {
	Count       int32                      `json:count`
	Next        *string                    `json:next`
	Previous    *string                    `json:previous`
	Results     []*Node                    `json:results`
	Request     *http.Request              `json:-`
	Response    *http.Response             `json:-`
	Error       error                      `json:-`
}

type MemberListResult struct {
	Count       int32                      `json:count`
	Next        *string                    `json:next`
	Previous    *string                    `json:previous`
	Results     []*Member                  `json:results`
	Request     *http.Request              `json:-`
	Response    *http.Response             `json:-`
	Error       error                      `json:-`
}

// BagListResult is what the REST service returns when
// we ask for a list of bags.
type BagListResult struct {
	Count       int32                      `json:count`
	Next        *string                    `json:next`
	Previous    *string                    `json:previous`
	Results     []*DPNBag                  `json:results`
	Request     *http.Request              `json:-`
	Response    *http.Response             `json:-`
	Error       error                      `json:-`
}

// ReplicationListResult is what the REST service returns when
// we ask for a list of transfer requests.
type ReplicationListResult struct {
	Count       int32                     `json:count`
	Next        *string                   `json:next`
	Previous    *string                   `json:previous`
	Results     []*ReplicationTransfer    `json:results`
	Request     *http.Request             `json:-`
	Response    *http.Response            `json:-`
	Error       error                     `json:-`
}

// RestoreListResult is what the REST service returns when
// we ask for a list of restore requests.
type RestoreListResult struct {
	Count       int32                     `json:count`
	Next        *string                   `json:next`
	Previous    *string                   `json:previous`
	Results     []*RestoreTransfer        `json:results`
	Request     *http.Request             `json:-`
	Response    *http.Response            `json:-`
	Error       error                     `json:-`
}


// Creates a new DPN REST client.
func NewDPNRestClient(hostUrl, apiVersion, apiKey, node string, dpnConfig *models.DPNConfig) (*DPNRestClient, error) {
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for DPN REST client: %v", err)
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 8,
		DisableKeepAlives:   false,
	Dial: (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
		ResponseHeaderTimeout: 10 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	if dpnConfig.AcceptInvalidSSLCerts {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	httpClient := &http.Client{
		Jar: cookieJar,
		Transport: transport,
		CheckRedirect: RedirectHandler,
	}
	// Trim trailing slashes from host url
	for strings.HasSuffix(hostUrl, "/") {
		hostUrl = hostUrl[:len(hostUrl)-1]
	}
	client := &DPNRestClient{
		HostUrl: hostUrl,
		APIVersion: apiVersion,
		APIKey: apiKey,
		Node: node,
		dpnConfig: dpnConfig,
		httpClient: httpClient,
		transport: transport,
	}
	return client, nil
}


// BuildUrl combines the host and protocol in client.HostUrl with
// relativeUrl to create an absolute URL. For example, if client.HostUrl
// is "http://localhost:3456", then client.BuildUrl("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *DPNRestClient) BuildUrl(relativeUrl string, queryParams *url.Values) string {
	fullUrl := client.HostUrl + relativeUrl
	if queryParams != nil {
		fullUrl = fmt.Sprintf("%s?%s", fullUrl, queryParams.Encode())
	}
	return fullUrl
}

// newJsonGet returns a new request with headers indicating
// JSON request and response formats.
func (client *DPNRestClient) NewJsonRequest(method, targetUrl string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, targetUrl, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Token token=%s", client.APIKey))
	req.Header.Add("Connection", "Keep-Alive")
	return req, nil
}

func (client *DPNRestClient) MemberGet(identifier string) (*Member, error) {
	relativeUrl := fmt.Sprintf("/%s/member/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("MemberGet expected status 200 but got %d. URL: %s", response.StatusCode, objUrl)
		return nil, error
	}

	// Build and return the data structure
	obj := &Member{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

// Returns the DPN Member with the specified name, or an error
// if there is not exactly one DPN member with that name.
func (client *DPNRestClient) MemberGetByName(name string) (*Member, error) {
	params := url.Values{}
	params.Set("name", name)
	list, err := client.MemberListGet(&params)
	if err != nil {
		return nil, err
	}
	if list.Count == 0 {
		return nil, fmt.Errorf("Cannot find member with name '%s'", name)
	}
	if list.Count > 1 {
		return nil, fmt.Errorf("Found %d members with name '%s'", list.Count, name)
	}
	return list.Results[0], nil
}

func (client *DPNRestClient) MemberListGet(queryParams *url.Values) (*MemberListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/member/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("MemberListGet expected status 200 but got %d. URL: %s", response.StatusCode, objUrl)
		return nil, error
	}

	// Build and return the data structure
	result := &MemberListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}

func (client *DPNRestClient) MemberCreate(bag *Member) (*Member, error) {
	return client.dpnMemberSave(bag, "POST")
}

func (client *DPNRestClient) MemberUpdate(bag *Member) (*Member, error) {
	return client.dpnMemberSave(bag, "PUT")
}

func (client *DPNRestClient) dpnMemberSave(member *Member, method string) (*Member, error) {
	// POST/Create
	relativeUrl := fmt.Sprintf("/%s/member/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 201
	if method == "PUT" {
		// PUT/Update
		relativeUrl = fmt.Sprintf("/%s/member/%s/", client.APIVersion, member.UUID)
		objUrl = client.BuildUrl(relativeUrl, nil)
		expectedResponseCode = 200
	}
	postData, err := json.Marshal(member)
	if err != nil {
		return nil, err
	}
	req, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("%s to %s returned status code %d. Post data: %v",
			method, objUrl, response.StatusCode, string(postData))
		return nil, error
	}
	returnedMember := Member{}
	err = json.Unmarshal(body, &returnedMember)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from  %s", objUrl)
		return nil, error
	}
	return &returnedMember, nil
}

func (client *DPNRestClient) NodeGet(identifier string) (*Node, error) {
	relativeUrl := fmt.Sprintf("/%s/node/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("NodeGet expected status 200 but got %d. URL: %s", response.StatusCode, objUrl)
		return nil, error
	}

	// HACK! Get rid of this when Golang fixes the JSON null date problem!
	body = HackNullDates(body)

	// Build and return the data structure
	obj := &Node{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	obj.LastPullDate, err = client.NodeGetLastPullDate(identifier)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

func (client *DPNRestClient) NodeListGet(queryParams *url.Values) (*NodeListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/node/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("NodeListGet expected status 200 but got %d. URL: %s", response.StatusCode, objUrl)
		return nil, error
	}

	// HACK! Get rid of this when Golang fixes the JSON null date problem!
	body = HackNullDates(body)

	// Build and return the data structure
	result := &NodeListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}


// NodeUpdate updates a DPN Node record. You can update node
// records only if you are the admin on the server where you're
// updating the record. Though this method lets you update any
// attributes related to the node, you should update only the
// LastPullDate attribute through this client. Use the web admin
// interface to perform more substantive node updates.
func (client *DPNRestClient) NodeUpdate(node *Node) (*Node, error) {
	relativeUrl := fmt.Sprintf("/%s/node/%s/", client.APIVersion, node.Namespace)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 200
	postData, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}
	req, err := client.NewJsonRequest("PUT", objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("PUT to %s returned status code %d", objUrl, response.StatusCode)
		return nil, error
	}

	// HACK! Get rid of this when Golang fixes the JSON null date problem!
	body = HackNullDates(body)

	returnedNode := Node{}
	err = json.Unmarshal(body, &returnedNode)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from  %s", objUrl)
		return nil, error
	}
	return &returnedNode, nil
}

// Returns the last time we pulled data from the specified node.
func (client *DPNRestClient) NodeGetLastPullDate(identifier string) (time.Time, error) {
	params := url.Values{}
	params.Set("ordering", "updated_at")
	params.Set("page", "1")
	params.Set("page_size", "1")
	bags, err := client.DPNBagListGet(&params)
	if err != nil || bags.Count == 0 {
		return time.Time{}, err
	}
	return bags.Results[0].UpdatedAt, err
}

func (client *DPNRestClient) DPNBagGet(identifier string) (*DPNBag, error) {
	relativeUrl := fmt.Sprintf("/%s/bag/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("DPNBagGet expected status 200 but got %d. URL: %s", response.StatusCode, objUrl)
		return nil, error
	}

	// Build and return the data structure
	obj := &DPNBag{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

func (client *DPNRestClient) DPNBagListGet(queryParams *url.Values) (*BagListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/bag/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("DPNBagListGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		return nil, error
	}

	// Build and return the data structure
	result := &BagListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}


func (client *DPNRestClient) DPNBagCreate(bag *DPNBag) (*DPNBag, error) {
	return client.dpnBagSave(bag, "POST")
}

func (client *DPNRestClient) DPNBagUpdate(bag *DPNBag) (*DPNBag, error) {
	return client.dpnBagSave(bag, "PUT")
}

func (client *DPNRestClient) dpnBagSave(bag *DPNBag, method string) (*DPNBag, error) {
	// POST/Create
	relativeUrl := fmt.Sprintf("/%s/bag/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 201
	if method == "PUT" {
		// PUT/Update
		relativeUrl = fmt.Sprintf("/%s/bag/%s/", client.APIVersion, bag.UUID)
		objUrl = client.BuildUrl(relativeUrl, nil)
		expectedResponseCode = 200
	}
	postData, err := json.Marshal(bag)
	if err != nil {
		return nil, err
	}
	req, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("%s to %s returned status code %d. Post data: %v",
			method, objUrl, response.StatusCode, string(postData))
		return nil, error
	}
	returnedBag := DPNBag{}
	err = json.Unmarshal(body, &returnedBag)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from  %s", objUrl)
		return nil, error
	}
	return &returnedBag, nil
}

func (client *DPNRestClient) ReplicationTransferGet(identifier string) (*ReplicationTransfer, error) {
	// /api-v1/replicate/aptrust-999999/
	relativeUrl := fmt.Sprintf("/%s/replicate/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("ReplicationTransferGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		return nil, error
	}

	// Build and return the data structure
	obj := &ReplicationTransfer{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

func (client *DPNRestClient) ReplicationListGet(queryParams *url.Values) (*ReplicationListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/replicate/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("ReplicationListGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		return nil, error
	}

	// Build and return the data structure
	result := &ReplicationListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}


func (client *DPNRestClient) ReplicationTransferCreate(xfer *ReplicationTransfer) (*ReplicationTransfer, error) {
	return client.replicationTransferSave(xfer, "POST")
}

func (client *DPNRestClient) ReplicationTransferUpdate(xfer *ReplicationTransfer) (*ReplicationTransfer, error) {
	return client.replicationTransferSave(xfer, "PUT")
}

func (client *DPNRestClient) replicationTransferSave(xfer *ReplicationTransfer, method string) (*ReplicationTransfer, error) {
	// POST/Create
	relativeUrl := fmt.Sprintf("/%s/replicate/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 201
	if method == "PUT" {
		// PUT/Update
		relativeUrl = fmt.Sprintf("/%s/replicate/%s/", client.APIVersion, xfer.ReplicationId)
		objUrl = client.BuildUrl(relativeUrl, nil)
		expectedResponseCode = 200
	}
	xfer.UpdatedAt = time.Now().UTC().Truncate(time.Second)
	postData, err := json.Marshal(xfer)
	if err != nil {
		return nil, err
	}

	req, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("%s to %s returned status code %d. Post data: %v",
			method, objUrl, response.StatusCode, string(postData))
		return nil, error
	}
	returnedXfer := ReplicationTransfer{}
	err = json.Unmarshal(body, &returnedXfer)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from %s: %v", objUrl, err)
		return nil, error
	}
	return &returnedXfer, nil
}

func (client *DPNRestClient) RestoreTransferGet(identifier string) (*RestoreTransfer, error) {
	// /api-v1/restore/aptrust-64/
	relativeUrl := fmt.Sprintf("/%s/restore/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("RestoreTransferGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		return nil, error
	}

	// Build and return the data structure
	obj := &RestoreTransfer{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

func (client *DPNRestClient) RestoreListGet(queryParams *url.Values) (*RestoreListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/restore/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("RestoreListGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		return nil, error
	}

	// Build and return the data structure
	result := &RestoreListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}

func (client *DPNRestClient) RestoreTransferCreate(xfer *RestoreTransfer) (*RestoreTransfer, error) {
	return client.restoreTransferSave(xfer, "POST")
}

func (client *DPNRestClient) RestoreTransferUpdate(xfer *RestoreTransfer) (*RestoreTransfer, error) {
	return client.restoreTransferSave(xfer, "PUT")
}

func (client *DPNRestClient) restoreTransferSave(xfer *RestoreTransfer, method string) (*RestoreTransfer, error) {
	// POST/Create
	relativeUrl := fmt.Sprintf("/%s/restore/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 201
	if method == "PUT" {
		// PUT/Update
		relativeUrl = fmt.Sprintf("/%s/restore/%s/", client.APIVersion, xfer.RestoreId)
		objUrl = client.BuildUrl(relativeUrl, nil)
		expectedResponseCode = 200
	}
	postData, err := json.Marshal(xfer)
	if err != nil {
		return nil, err
	}
	req, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("%s to %s returned status code %d. Post data: %v",
			method, objUrl, response.StatusCode, string(postData))
		return nil, error
	}

	returnedXfer := RestoreTransfer{}
	err = json.Unmarshal(body, &returnedXfer)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from  %s", objUrl)
		return nil, error
	}
	return &returnedXfer, nil
}


// Returns a DPN REST client that can talk to a remote node.
// This function has to connect to out local DPN node to get
// information about the remote node. It returns a new client
// that can connect to the remote node with the correct URL
// and API key. We use this function to get a client that can
// update a replication request or a restore request on the
// originating node.
func (client *DPNRestClient) GetRemoteClient(remoteNodeNamespace string, dpnConfig *models.DPNConfig) (*DPNRestClient, error) {
	remoteNode, err := client.NodeGet(remoteNodeNamespace)
	if err != nil {
		detailedError := fmt.Errorf("Error retrieving node record for '%s' "+
			"from local DPN REST service: %v", remoteNodeNamespace, err)
		return nil, detailedError
	}

	authToken := dpnConfig.RemoteNodeTokens[remoteNode.Namespace]
	if authToken == "" {
		detailedError := fmt.Errorf("Cannot get auth token for node %s", remoteNode.Namespace)
		return nil, detailedError
	}
	apiRoot := remoteNode.APIRoot
	// Overriding DPN REST client URL if DPNConfig.RemoteNodeURLs says so
	if dpnConfig.RemoteNodeURLs != nil && dpnConfig.RemoteNodeURLs[remoteNodeNamespace] != "" {
		apiRoot = dpnConfig.RemoteNodeURLs[remoteNodeNamespace]
	}
	remoteRESTClient, err := NewDPNRestClient(
		apiRoot,
		dpnConfig.RestClient.LocalAPIRoot, // All nodes should be on same version
		authToken,
		remoteNodeNamespace,
		dpnConfig)
	if err != nil {
		detailedError := fmt.Errorf("Could not create REST client for remote node %s: %v",
			remoteNode.Namespace, err)
		return nil, detailedError
	}
	return remoteRESTClient, nil
}

// Reads the response body and returns a byte slice.
// You must read and close the response body, or the
// TCP connection will remain open for as long as
// our application runs.
func readResponse(body io.ReadCloser) (data []byte, err error) {
	if body != nil {
		data, err = ioutil.ReadAll(body)
		body.Close()
	}
	return data, err
}

func (client *DPNRestClient) doRequest(request *http.Request) (data []byte, response *http.Response, err error) {
	response, err = client.httpClient.Do(request)
	if err != nil {
		return nil, nil, err
	}
	data, err = readResponse(response.Body)
	if err != nil {
		return nil, response, err
	}
	return data, response, err
}


func (client *DPNRestClient) formatJsonError(callerName string, body []byte, err error) (error) {
	json := strings.Replace(string(body), "\n", " ", -1)
	return fmt.Errorf("%s: Error parsing JSON response: %v -- JSON response: %s", err, json)
}

// This hack works around the JSON decoding bug in Golang's core
// time and json libraries. The bug is described here:
// https://go-review.googlesource.com/#/c/9376/
// We could do a "proper" work-around by changing all our structs
// to use pointers to time.Time instead of time.Time values, but then
// we have to check for nil in many places. There's already a patch
// in to fix this bug in the next release of Golang, so
// I'd rather have this hack for now and remove it when the next
// version of Golang comes out. That's better than changing to pointers,
// checking for nil in a hundred places, and then reverting ALL THAT when
// the bug is fixed. In practice the only null dates that should ever come
// out of our REST services are Node.LastPullDate, and that should only
// happen on the first day a new node is up and runnning. We just have
// to set these back to a reasonably old timestamp so we can ask the
// node for all items updated since that time. "Reasonably old" is
// anything before about June 1, 2015.
func HackNullDates(jsonBytes []byte)([]byte) {
	// Problem fixed with regex == two problems
	dummyDate := "\"last_pull_date\":\"1980-01-01T00:00:00Z\""
	re := regexp.MustCompile("\"last_pull_date\":\\s*null")
	return re.ReplaceAll(jsonBytes, []byte(dummyDate))
}


// By default, the Go HTTP client does not send headers from the
// original request to the redirect location. See the issue at
// https://code.google.com/p/go/issues/detail?id=4800&q=request%20header
//
// We want to send all headers from the original request, but we'll
// send the auth header only if the host of the redirect URL matches
// the host of the original URL.
func RedirectHandler (req *http.Request, via []*http.Request) (error) {
	if len(via) >= 10 {
		return fmt.Errorf("too many redirects")
	}
	if len(via) == 0 {
		return nil
	}
	for attr, val := range via[0].Header {
		if _, ok := req.Header[attr]; !ok {
			// Copy all headers except Authorization from the original request.
			// If the new URL is at the same host as the original URL,
			// copy the Auth header as well.
			if attr != "Authorization" || req.URL.Host == via[0].URL.Host {
				req.Header[attr] = val
			}
		}
	}
	return nil
}
