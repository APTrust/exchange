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
// Common params for "List" methods include page (the page number
// in a paged result set), page_size (the number of results per
// page to retrieve), order_by (which can be created_at or updated_at,
// and always returns results in descending order), before (which
// includes only items whose updated_at is before the specified
// timestamp) and after (which includes only items whose updated_at
// is after the specified timestamp). Additional information about
// the DPN server and its capabilities are available at
// http://chronopolis01.umiacs.umd.edu/ or any swagger server that points to
// https://raw.githubusercontent.com/dpn-admin/dpn-rest-spec/master/dist/swagger.yaml
//
// The main dpn-server repo is available at
// https://github.com/dpn-admin/dpn-server
type DPNRestClient struct {
	HostUrl      string
	APIVersion   string
	APIKey       string
	Node         string
	dpnConfig    models.DPNConfig
	httpClient   *http.Client
	transport    *http.Transport
}

// NewDPNRestClient creates a new DPN REST client.
func NewDPNRestClient(hostUrl, apiVersion, apiKey, node string, dpnConfig models.DPNConfig) (*DPNRestClient, error) {
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

// GetRemoteClients returns a map of clients that can connect to remote
// DPN REST services. These clients are used to pull data from other nodes
// and to update the status of replication and restore requests on other
// nodes. The key in the returned map is the remote node's namespace. The
// value is a pointer to a client object that connects to that node.
//
// This will return ONLY those clients for whom the config file contains
// a RemoteNodeToken entry, because it's impossible to connect to a remote
// node without a token.
func (client *DPNRestClient) GetRemoteClients() (map[string]*DPNRestClient, error) {
	remoteClients := make(map[string]*DPNRestClient)
	for namespace, _ := range client.dpnConfig.RemoteNodeTokens {
		remoteClient, err := client.GetRemoteClient(namespace, client.dpnConfig)
		if err != nil {
			return nil, fmt.Errorf("Error creating remote client for node %s: %v", namespace, err)
		}
		remoteClients[namespace] = remoteClient
	}
	return remoteClients, nil
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

// MemberGet returns a DPNResponse containing the member with the
// specified identifier, if that member exists.
func (client *DPNRestClient) MemberGet(identifier string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeMember)
	resp.members = make([]*Member, 1)

	relativeUrl := fmt.Sprintf("/%s/member/%s/", client.APIVersion, identifier)
	absUrl := client.BuildUrl(relativeUrl, nil)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	member := &Member{}
	resp.Error = json.Unmarshal(resp.data, member)
	if resp.Error == nil {
		resp.members[0] = member
	}
	return resp
}

// MemberList returns a DPNResponse members that match the specific
// params. Valid params include before, after, page, page_size
// and order_by.
func (client *DPNRestClient) MemberList(params *url.Values) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeMember)
	resp.members = make([]*Member, 1)

	relativeUrl := fmt.Sprintf("/%s/member/", client.APIVersion)
	absUrl := client.BuildUrl(relativeUrl, params)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}
	resp.UnmarshalJsonList()
	return resp
}

// MemberCreate creates a new member in the DPN repository.
func (client *DPNRestClient) MemberCreate(member *Member) (*DPNResponse) {
	return client.dpnMemberSave(member, "POST")
}

// MemberUpdate creates a new member in the DPN repository.
func (client *DPNRestClient) MemberUpdate(member *Member) (*DPNResponse) {
	return client.dpnMemberSave(member, "PUT")
}

// dpnMemberSave creates or updates a member in the DPN repository,
// depending on the httpMethod.
func (client *DPNRestClient) dpnMemberSave(member *Member, httpMethod string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeMember)
	resp.members = make([]*Member, 1)

	relativeUrl := fmt.Sprintf("/%s/member/", client.APIVersion)
	if httpMethod == "PUT" {
		relativeUrl = fmt.Sprintf("/%s/member/%s", client.APIVersion, member.MemberId)
	}
	absoluteUrl := client.BuildUrl(relativeUrl, nil)

	// Create the JSON data
	postData, err := json.Marshal(member)
	if err != nil {
		resp.Error = err
	}

	// Build the request
	client._doRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedMember := &Member{}
	resp.Error = json.Unmarshal(resp.data, savedMember)
	if resp.Error == nil {
		resp.members[0] = savedMember
	}
	return resp
}

// NodeGet returns the node with the specified identifier (namespace).
func (client *DPNRestClient) NodeGet(identifier string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeNode)
	resp.nodes = make([]*Node, 1)

	relativeUrl := fmt.Sprintf("/%s/node/%s/", client.APIVersion, identifier)
	absUrl := client.BuildUrl(relativeUrl, nil)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	node := &Node{}
	resp.Error = json.Unmarshal(resp.data, node)
	if resp.Error == nil {
		node.LastPullDate, resp.Error = client.NodeGetLastPullDate(identifier)
		resp.nodes[0] = node
	}
	return resp
}

// NodeList returns a DPNResponse containing nodes that match the
// specified params. Valid params include before, after, page,
// page_size, and order_by. This call is deprecated in DPN 2.0
// and may disappear in later versions.
func (client *DPNRestClient) NodeList(params *url.Values) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeNode)
	resp.nodes = make([]*Node, 1)

	relativeUrl := fmt.Sprintf("/%s/node/", client.APIVersion)
	absUrl := client.BuildUrl(relativeUrl, params)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}
	resp.UnmarshalJsonList()
	return resp
}

// NodeUpdate updates a DPN Node record. You can update node
// records only if you are the admin on the server where you're
// updating the record. Though this method lets you update any
// attributes related to the node, you should update only the
// LastPullDate attribute through this client. Use the web admin
// interface to perform more substantive node updates.
func (client *DPNRestClient) NodeUpdate(node *Node) (*DPNResponse) {
	return client.nodeSave(node, "PUT")
}

// nodeSave creates or updates a node. Since the current DPN REST API
// does not support creating new nodes, this client doesn't implement
// NodeCreate.
func (client *DPNRestClient) nodeSave(node *Node, httpMethod string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeNode)
	resp.nodes = make([]*Node, 1)

	relativeUrl := fmt.Sprintf("/%s/node/", client.APIVersion)
	if httpMethod == "PUT" {
		relativeUrl = fmt.Sprintf("/%s/node/%s", client.APIVersion, node.Namespace)
	}
	absoluteUrl := client.BuildUrl(relativeUrl, nil)

	// Create the JSON data
	postData, err := json.Marshal(node)
	if err != nil {
		resp.Error = err
	}

	// Build the request
	client._doRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedNode := &Node{}
	resp.Error = json.Unmarshal(resp.data, savedNode)
	if resp.Error == nil {
		resp.nodes[0] = savedNode
	}
	return resp
}

// NodeGetLastPullDate returns the last time we pulled data from the
// specified node. The last pull date is derived from the latest
// updated_at timestamp for bags from the specified admin_node.
func (client *DPNRestClient) NodeGetLastPullDate(identifier string) (time.Time, error) {
	params := url.Values{}
	params.Set("admin_node", identifier)
	params.Set("order_by", "updated_at")
	params.Set("page", "1")
	params.Set("page_size", "1")
	resp := client.DPNBagList(&params)
	if resp.Error != nil || resp.Count == 0 {
		return time.Time{}, resp.Error
	}
	return resp.Bags()[0].UpdatedAt, nil
}

// DPNBagGet returns a DPNResponse with the bag having the specified
// identifier, if it exists.
func (client *DPNRestClient) DPNBagGet(identifier string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeBag)
	resp.bags = make([]*DPNBag, 1)

	relativeUrl := fmt.Sprintf("/%s/bag/%s/", client.APIVersion, identifier)
	absUrl := client.BuildUrl(relativeUrl, nil)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	bag := &DPNBag{}
	resp.Error = json.Unmarshal(resp.data, bag)
	if resp.Error == nil {
		resp.bags[0] = bag
	}
	return resp
}

// DPNBagList lists bags matching the specified parameters.
// Valid parameters include before, after, bag_type, admin_node,
// ingest_node, member, replicated_by, first_version_uuid, page,
// page_size, order_by.
func (client *DPNRestClient) DPNBagList(params *url.Values) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeBag)
	resp.bags = make([]*DPNBag, 1)

	relativeUrl := fmt.Sprintf("/%s/bag/", client.APIVersion)
	absUrl := client.BuildUrl(relativeUrl, params)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}
	resp.UnmarshalJsonList()
	return resp
}

// DPNBagCreate creates a new bag. Note that you can create bags
// only at your own node.
func (client *DPNRestClient) DPNBagCreate(bag *DPNBag) (*DPNResponse) {
	return client.dpnBagSave(bag, "POST")
}

// DPNBagUpdate updates an existing bag. Note that you can update bags
// only at your own node.
func (client *DPNRestClient) DPNBagUpdate(bag *DPNBag) (*DPNResponse) {
	return client.dpnBagSave(bag, "PUT")
}

// dpnBagSave saves a bag record.
func (client *DPNRestClient) dpnBagSave(bag *DPNBag, httpMethod string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeBag)
	resp.bags = make([]*DPNBag, 1)

	relativeUrl := fmt.Sprintf("/%s/bag/", client.APIVersion)
	if httpMethod == "PUT" {
		relativeUrl = fmt.Sprintf("/%s/bag/%s", client.APIVersion, bag.UUID)
	}
	absoluteUrl := client.BuildUrl(relativeUrl, nil)

	// Create the JSON data
	postData, err := json.Marshal(bag)
	if err != nil {
		resp.Error = err
	}

	// Build the request
	client._doRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedBag := &DPNBag{}
	resp.Error = json.Unmarshal(resp.data, savedBag)
	if resp.Error == nil {
		resp.bags[0] = savedBag
	}
	return resp
}

// ReplicationTransferGet returns the ReplicationTransfer with the
// specified id, if it exists.
func (client *DPNRestClient) ReplicationTransferGet(identifier string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeReplication)
	resp.replications = make([]*ReplicationTransfer, 1)

	relativeUrl := fmt.Sprintf("/%s/replicate/%s/", client.APIVersion, identifier)
	absUrl := client.BuildUrl(relativeUrl, nil)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	replication := &ReplicationTransfer{}
	resp.Error = json.Unmarshal(resp.data, replication)
	if resp.Error == nil {
		resp.replications[0] = replication
	}
	return resp
}

// ReplicationList returns a list of ReplicationTransfers matching
// the specified criteria. Valid params include before, after, bag,
// to_node, from_node, store_requested, stored, cancelled, cancel_reason,
// page, page_size, order_by.
func (client *DPNRestClient) ReplicationList(params *url.Values) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeReplication)
	resp.replications = make([]*ReplicationTransfer, 1)

	relativeUrl := fmt.Sprintf("/%s/replicate/", client.APIVersion)
	absUrl := client.BuildUrl(relativeUrl, params)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}
	resp.UnmarshalJsonList()
	return resp
}

// ReplicationTransferCreate creates a ReplicationTransfer. You can only
// create transfers on your own node.
func (client *DPNRestClient) ReplicationTransferCreate(xfer *ReplicationTransfer) (*DPNResponse) {
	return client.replicationTransferSave(xfer, "POST")
}

// ReplicationTransferUpdate updates a ReplicationTransfer. You can
// updated transfers on remote nodes if they are the from_node and you
// are the to_node.
func (client *DPNRestClient) ReplicationTransferUpdate(xfer *ReplicationTransfer) (*DPNResponse) {
	return client.replicationTransferSave(xfer, "PUT")
}

// replicationTransferSave saves a ReplicationTransfer.
func (client *DPNRestClient) replicationTransferSave(xfer *ReplicationTransfer, httpMethod string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeReplication)
	resp.replications = make([]*ReplicationTransfer, 1)

	relativeUrl := fmt.Sprintf("/%s/replicate/", client.APIVersion)
	if httpMethod == "PUT" {
		relativeUrl = fmt.Sprintf("/%s/replicate/%s", client.APIVersion, xfer.ReplicationId)
	}
	absoluteUrl := client.BuildUrl(relativeUrl, nil)

	// Create the JSON data
	postData, err := json.Marshal(xfer)
	if err != nil {
		resp.Error = err
	}

	// Build the request
	client._doRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedReplication := &ReplicationTransfer{}
	resp.Error = json.Unmarshal(resp.data, savedReplication)
	if resp.Error == nil {
		resp.replications[0] = savedReplication
	}
	return resp
}

// RestoreTransferGet returns the RestoreTransfer with the specified
// identifier.
func (client *DPNRestClient) RestoreTransferGet(identifier string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeRestore)
	resp.restores = make([]*RestoreTransfer, 1)

	relativeUrl := fmt.Sprintf("/%s/restore/%s/", client.APIVersion, identifier)
	absUrl := client.BuildUrl(relativeUrl, nil)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	restore := &RestoreTransfer{}
	resp.Error = json.Unmarshal(resp.data, restore)
	if resp.Error == nil {
		resp.restores[0] = restore
	}
	return resp
}

// RestoreTransferList returns a list of RestoreTransfers matching the
// specified criteria. Valid params include before, after, bag, to_node,
// from_node, accepted, finished, cancelled, cancel_reason, page, page_size,
// order_by.
func (client *DPNRestClient) RestoreTransferList(params *url.Values) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeRestore)
	resp.restores = make([]*RestoreTransfer, 1)

	relativeUrl := fmt.Sprintf("/%s/restore/", client.APIVersion)
	absUrl := client.BuildUrl(relativeUrl, params)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}
	resp.UnmarshalJsonList()
	return resp
}

// RestoreTransferCreate creates a RestoreTransfer request, which you can
// do only on your own node.
func (client *DPNRestClient) RestoreTransferCreate(xfer *RestoreTransfer) (*DPNResponse) {
	return client.restoreTransferSave(xfer, "POST")
}

// RestoreTransferUpdate updates a RestoreTransfer request, which you can do
// on your own node if you are the to_node, or on the to_node if you are the
// from_node.
func (client *DPNRestClient) RestoreTransferUpdate(xfer *RestoreTransfer) (*DPNResponse) {
	return client.restoreTransferSave(xfer, "PUT")
}

// restoreTransferSave saves a RestoreTransfer.
func (client *DPNRestClient) restoreTransferSave(xfer *RestoreTransfer, httpMethod string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeRestore)
	resp.restores = make([]*RestoreTransfer, 1)

	relativeUrl := fmt.Sprintf("/%s/restore/", client.APIVersion)
	if httpMethod == "PUT" {
		relativeUrl = fmt.Sprintf("/%s/restore/%s", client.APIVersion, xfer.RestoreId)
	}
	absoluteUrl := client.BuildUrl(relativeUrl, nil)

	// Create the JSON data
	postData, err := json.Marshal(xfer)
	if err != nil {
		resp.Error = err
	}

	// Build the request
	client._doRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedRestore := &RestoreTransfer{}
	resp.Error = json.Unmarshal(resp.data, savedRestore)
	if resp.Error == nil {
		resp.restores[0] = savedRestore
	}
	return resp
}

// DigestGet returns the message digest for the specified bag with
// the specified algorithm, if it exists.
func (client *DPNRestClient) DigestGet(bagUUID, algorithm string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeDigest)
	resp.digests = make([]*MessageDigest, 1)

	relativeUrl := fmt.Sprintf("/%s/bag/%s/digest/%s/", client.APIVersion, bagUUID, algorithm)
	absUrl := client.BuildUrl(relativeUrl, nil)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	digest := &MessageDigest{}
	resp.Error = json.Unmarshal(resp.data, digest)
	if resp.Error == nil {
		resp.digests[0] = digest
	}
	return resp
}

// DigestList returns a list of MessageDigests that match the specified
// criteria. Param uuid (the uuid of the bag to which the digests belong)
// is required. Optional params include before, after, page, page_size,
// and order_by.
func (client *DPNRestClient) DigestList(params *url.Values) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeDigest)
	resp.digests = make([]*MessageDigest, 1)

	relativeUrl := fmt.Sprintf("/%s/digest/", client.APIVersion)
	absUrl := client.BuildUrl(relativeUrl, params)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}
	resp.UnmarshalJsonList()
	return resp
}

// DigestCreate creates a MessageDigest record.
func (client *DPNRestClient) DigestCreate(digest *MessageDigest) (*DPNResponse) {
	return client.digestSave(digest, "POST")
}

// digestSave saves a MessageDigest record.
// Note that the DPN 2.0 server does not implement DigestUpdate.
func (client *DPNRestClient) digestSave(digest *MessageDigest, httpMethod string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeDigest)
	resp.digests = make([]*MessageDigest, 1)

	relativeUrl := fmt.Sprintf("/%s/bag/%s/digest", client.APIVersion, digest.Bag)
	absoluteUrl := client.BuildUrl(relativeUrl, nil)

	// Create the JSON data
	postData, err := json.Marshal(digest)
	if err != nil {
		resp.Error = err
	}

	// Build the request
	client._doRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedDigest := &MessageDigest{}
	resp.Error = json.Unmarshal(resp.data, savedDigest)
	if resp.Error == nil {
		resp.digests[0] = savedDigest
	}
	return resp
}

// FixityCheckList returns a list of FixityCheck records. Valid params include
// before, after, bag, latest, node, page, page_size, order_by. Param latest
// is a boolean. If true, only the latest fixity check(s) for each bag will
// be returned. Note that the DPN 2.0 server does not implement FixityCheckGet.
func (client *DPNRestClient) FixityCheckList(params *url.Values) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeFixityCheck)
	resp.fixities = make([]*FixityCheck, 1)

	relativeUrl := fmt.Sprintf("/%s/fixity_check/", client.APIVersion)
	absUrl := client.BuildUrl(relativeUrl, params)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}
	resp.UnmarshalJsonList()
	return resp
}

// FixityCheckCreate creates a new FixityCheck
func (client *DPNRestClient) FixityCheckCreate(fixity *FixityCheck) (*DPNResponse) {
	return client.fixityCheckSave(fixity, "POST")
}

// fixityCheckSave saves a FixityCheck via POST or PUT.
// Note that the DPN 2.0 server does not implement FixityCheckUpdate.
func (client *DPNRestClient) fixityCheckSave(fixity *FixityCheck, httpMethod string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeFixityCheck)
	resp.fixities = make([]*FixityCheck, 1)

	relativeUrl := fmt.Sprintf("/%s/fixity_check/", client.APIVersion)
	absoluteUrl := client.BuildUrl(relativeUrl, nil)

	// Create the JSON data
	postData, err := json.Marshal(fixity)
	if err != nil {
		resp.Error = err
	}

	// Build the request
	client._doRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedFixity := &FixityCheck{}
	resp.Error = json.Unmarshal(resp.data, savedFixity)
	if resp.Error == nil {
		resp.fixities[0] = savedFixity
	}
	return resp
}

// IngestList returns a list of Ingest records that match the specified
// criteria. Valid params include before, after, bag, ingested, latest,
// page, page_size, order_by. See the swagger docs for more info.
// Note that the DPN 2.0 server does not implement IngestGet.
func (client *DPNRestClient) IngestList(params *url.Values) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeIngest)
	resp.ingests = make([]*Ingest, 1)

	relativeUrl := fmt.Sprintf("/%s/ingest/", client.APIVersion)
	absUrl := client.BuildUrl(relativeUrl, params)

	client._doRequest(resp, "GET", absUrl, nil)
	if resp.Error != nil {
		return resp
	}
	resp.UnmarshalJsonList()
	return resp
}

// IngestCreate creates a new Ingest record.
func (client *DPNRestClient) IngestCreate(ingest *Ingest) (*DPNResponse) {
	return client.ingestSave(ingest, "POST")
}

// ingestSave saves an Ingest record by POST or PUT.
// Note that the DPN 2.0 server does not implement IngestUpdate.
func (client *DPNRestClient) ingestSave(ingest *Ingest, httpMethod string) (*DPNResponse) {
	resp := NewDPNResponse(DPNTypeIngest)
	resp.ingests = make([]*Ingest, 1)

	relativeUrl := fmt.Sprintf("/%s/ingest/", client.APIVersion)
	absoluteUrl := client.BuildUrl(relativeUrl, nil)

	// Create the JSON data
	postData, err := json.Marshal(ingest)
	if err != nil {
		resp.Error = err
	}

	// Build the request
	client._doRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedIngest := &Ingest{}
	resp.Error = json.Unmarshal(resp.data, savedIngest)
	if resp.Error == nil {
		resp.ingests[0] = savedIngest
	}
	return resp
}

// Returns a DPN REST client that can talk to a remote node.
// This function has to connect to out local DPN node to get
// information about the remote node. It returns a new client
// that can connect to the remote node with the correct URL
// and API key. We use this function to get a client that can
// update a replication request or a restore request on the
// originating node.
func (client *DPNRestClient) GetRemoteClient(remoteNodeNamespace string, dpnConfig models.DPNConfig) (*DPNRestClient, error) {
	nodeResult := client.NodeGet(remoteNodeNamespace)
	if nodeResult.Error != nil {
		detailedError := fmt.Errorf("Error retrieving node record for '%s' "+
			"from local DPN REST service: %v", remoteNodeNamespace, nodeResult.Error)
		return nil, detailedError
	}
	remoteNode := nodeResult.Node()
	if remoteNode == nil {
		detailedError := fmt.Errorf("Local DPN REST service has no record of node %d",
			remoteNodeNamespace)
		return nil, detailedError
	}

	authToken := dpnConfig.RemoteNodeTokens[remoteNode.Namespace]
	if authToken == "" {
		detailedError := fmt.Errorf("Cannot get auth token for node %s", remoteNode.Namespace)
		return nil, detailedError
	}
	apiRoot := remoteNode.APIRoot

	// Overriding DPN REST client URL if DPNConfig.RemoteNodeURLs says so.
	// This is used in testing, when we want to configure remote node URL
	// to point to a service on the local system.
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

// func (client *DPNRestClient) doRequest(request *http.Request) (data []byte, response *http.Response, err error) {
//	response, err = client.httpClient.Do(request)
//	if err != nil {
//		return nil, nil, err
//	}
//	data, err = readResponse(response.Body)
//	if err != nil {
//		return nil, response, err
//	}
//	return data, response, err
// }

// DoRequest issues an HTTP request, reads the response, and closes the
// connection to the remote server.
//
// Param resp should be a DPNResponse.
//
// For a description of the other params, see NewJsonRequest.
//
// If an error occurs, it will be recorded in resp.Error.
func (client *DPNRestClient) _doRequest(resp *DPNResponse, method, absoluteUrl string, requestData io.Reader) {
	// Build the request
	request, err := client.NewJsonRequest(method, absoluteUrl, requestData)
	resp.Request = request
	resp.Error = err
	if resp.Error != nil {
		return
	}

	// Issue the HTTP request
	resp.Response, resp.Error = client.httpClient.Do(request)
	if resp.Error != nil {
		return
	}

	// Read the response data and close the response body.
	// That's the only way to close the remote HTTP connection,
	// which will otherwise stay open indefinitely, causing
	// the system to eventually have too many open files.
	// If there's an error reading the response body, it will
	// be recorded in resp.Error.
	resp.readResponse()
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
