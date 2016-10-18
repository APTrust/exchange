package dpn

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

type DPNResponse struct {
	Count             int
	Next              *string
	Previous          *string
	Request           *http.Request
	Response          *http.Response
	Error             error

	bags              []*DPNBag
	digests           []*MessageDigest
	fixities          []*FixityCheck
	ingests           []*Ingest
	members           []*Member
	nodes             []*Node
	replications      []*ReplicationTransfer
	restores          []*RestoreTransfer

	objectType        DPNObjectType
	hasBeenRead       bool
	listHasBeenParsed bool
	data              []byte
}

type DPNObjectType string

const (
	DPNTypeBag         DPNObjectType = "DPNBag"
	DPNTypeDigest                    = "Digest"
	DPNTypeFixityCheck               = "FixityCheck"
	DPNTypeIngest                    = "Ingest"
	DPNTypeMember                    = "Member"
	DPNTypeNode                      = "Node"
	DPNTypeReplication               = "Replication"
	DPNTypeRestore                   = "Restore"
)

var DPNTypes = []DPNObjectType{
	DPNTypeBag,
	DPNTypeDigest,
	DPNTypeFixityCheck,
	DPNTypeIngest,
	DPNTypeMember,
	DPNTypeNode,
	DPNTypeReplication,
	DPNTypeRestore,
}

// NewDPNResponse returns a pointer to a new response object.
func NewDPNResponse(objType DPNObjectType) (*DPNResponse) {
	return &DPNResponse{
		Count: 0,
		Next: nil,
		Previous: nil,
		objectType: objType,
		hasBeenRead: false,
		listHasBeenParsed: false,
	}
}

// Returns the raw body of the HTTP response as a byte slice.
// The return value may be nil.
func (resp *DPNResponse) RawResponseData() ([]byte, error) {
	if !resp.hasBeenRead {
		resp.readResponse()
	}
	return resp.data, resp.Error
}

// Reads the body of an HTTP response object, closes the stream, and
// returns a byte array. The body MUST be closed, or you'll wind up
// with a lot of open network connections.
func (resp *DPNResponse) readResponse () {
	if !resp.hasBeenRead && resp.Response != nil && resp.Response.Body != nil {
		resp.data, resp.Error = ioutil.ReadAll(resp.Response.Body)
		resp.Response.Body.Close()
		resp.hasBeenRead = true
	}
}

// Returns the type of object(s) contained in this response.
func (resp *DPNResponse) ObjectType () (DPNObjectType) {
	return resp.objectType
}

// Returns true if the response includes a link to the next page
// of results.
func (resp *DPNResponse) HasNextPage() (bool) {
	return resp.Next != nil && *resp.Next != ""
}

// Returns true if the response includes a link to the previous page
// of results.
func (resp *DPNResponse) HasPreviousPage() (bool) {
	return resp.Previous != nil && *resp.Previous != ""
}

// Returns the URL parameters to request the next page of results,
// or nil if there is no next page.
func (resp *DPNResponse) ParamsForNextPage() (url.Values) {
	if resp.HasNextPage() {
		nextUrl, _ := url.Parse(*resp.Next)
		if nextUrl != nil {
			return nextUrl.Query()
		}
	}
	return nil
}

// Returns the URL parameters to request the previous page of results,
// or nil if there is no previous page.
func (resp *DPNResponse) ParamsForPreviousPage() (url.Values) {
	if resp.HasPreviousPage() {
		previousUrl, _ := url.Parse(*resp.Previous)
		if previousUrl != nil {
			return previousUrl.Query()
		}
	}
	return nil
}

// Returns the Bag parsed from the HTTP response body, or nil.
func (resp *DPNResponse) Bag() (*DPNBag) {
	if resp.bags != nil && len(resp.bags) > 0 {
		return resp.bags[0]
	}
	return nil
}

// Returns a list of Bags parsed from the HTTP response body.
func (resp *DPNResponse) Bags() ([]*DPNBag) {
	if resp.bags == nil {
		return make([]*DPNBag, 0)
	}
	return resp.bags
}

// Returns the Digest parsed from the HTTP response body, or nil.
func (resp *DPNResponse) Digest() (*MessageDigest) {
	if resp.digests != nil && len(resp.digests) > 0 {
		return resp.digests[0]
	}
	return nil
}

// Returns a list of Digests parsed from the HTTP response body.
func (resp *DPNResponse) Digests() ([]*MessageDigest) {
	if resp.digests == nil {
		return make([]*MessageDigest, 0)
	}
	return resp.digests
}

// Returns the FixityCheck parsed from the HTTP response body, or nil.
func (resp *DPNResponse) FixityCheck() (*FixityCheck) {
	if resp.fixities != nil && len(resp.fixities) > 0 {
		return resp.fixities[0]
	}
	return nil
}

// Returns a list of FixityChecks parsed from the HTTP response body.
func (resp *DPNResponse) FixityChecks() ([]*FixityCheck) {
	if resp.fixities == nil {
		return make([]*FixityCheck, 0)
	}
	return resp.fixities
}

// Returns the Ingest parsed from the HTTP response body, or nil.
func (resp *DPNResponse) Ingest() (*Ingest) {
	if resp.ingests != nil && len(resp.ingests) > 0 {
		return resp.ingests[0]
	}
	return nil
}

// Returns a list of Ingests parsed from the HTTP response body.
func (resp *DPNResponse) Ingests() ([]*Ingest) {
	if resp.ingests == nil {
		return make([]*Ingest, 0)
	}
	return resp.ingests
}

// Returns the Member parsed from the HTTP response body, or nil.
func (resp *DPNResponse) Member() (*Member) {
	if resp.members != nil && len(resp.members) > 0 {
		return resp.members[0]
	}
	return nil
}

// Returns a list of Members parsed from the HTTP response body.
func (resp *DPNResponse) Members() ([]*Member) {
	if resp.members == nil {
		return make([]*Member, 0)
	}
	return resp.members
}

// Returns the Node parsed from the HTTP response body, or nil.
func (resp *DPNResponse) Node() (*Node) {
	if resp.nodes != nil && len(resp.nodes) > 0 {
		return resp.nodes[0]
	}
	return nil
}

// Returns a list of Nodes parsed from the HTTP response body.
func (resp *DPNResponse) Nodes() ([]*Node) {
	if resp.nodes == nil {
		return make([]*Node, 0)
	}
	return resp.nodes
}

// Returns the ReplicationTransfer parsed from the HTTP response body, or nil.
func (resp *DPNResponse) ReplicationTransfer() (*ReplicationTransfer) {
	if resp.replications != nil && len(resp.replications) > 0 {
		return resp.replications[0]
	}
	return nil
}

// Returns a list of ReplicationTransfers parsed from the HTTP response body.
func (resp *DPNResponse) ReplicationTransfers() ([]*ReplicationTransfer) {
	if resp.replications == nil {
		return make([]*ReplicationTransfer, 0)
	}
	return resp.replications
}

// Returns the RestoreTransfer parsed from the HTTP response body, or nil.
func (resp *DPNResponse) RestoreTransfer() (*RestoreTransfer) {
	if resp.restores != nil && len(resp.restores) > 0 {
		return resp.restores[0]
	}
	return nil
}

// Returns a list of RestoreTransfers parsed from the HTTP response body.
func (resp *DPNResponse) RestoreTransfers() ([]*RestoreTransfer) {
	if resp.restores == nil {
		return make([]*RestoreTransfer, 0)
	}
	return resp.restores
}

func(resp *DPNResponse) UnmarshalJsonList() (error) {
	switch resp.objectType {
	case DPNTypeBag:
		return resp.unmarshalBagList()
	case DPNTypeDigest:
		return resp.unmarshalDigestList()
	case DPNTypeFixityCheck:
		return resp.unmarshalFixityList()
	case DPNTypeIngest:
		return resp.unmarshalIngestList()
	case DPNTypeMember:
		return resp.unmarshalMemberList()
	case DPNTypeNode:
		return resp.unmarshalNodeList()
	case DPNTypeReplication:
		return resp.unmarshalReplicationList()
	case DPNTypeRestore:
		return resp.unmarshalRestoreList()
	default:
		return fmt.Errorf("DPNObjectType %v not supported", resp.objectType)
	}
}

func(resp *DPNResponse) unmarshalBagList() (error) {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct{
		Count    int
		Next     *string
		Previous *string
		Results  []*DPNBag
	}{ 0, nil, nil, nil }
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.bags = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func(resp *DPNResponse) unmarshalDigestList() (error) {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct{
		Count    int
		Next     *string
		Previous *string
		Results  []*MessageDigest
	}{ 0, nil, nil, nil }
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.digests = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func(resp *DPNResponse) unmarshalFixityList() (error) {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct{
		Count    int
		Next     *string
		Previous *string
		Results  []*FixityCheck
	}{ 0, nil, nil, nil }
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.fixities = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func(resp *DPNResponse) unmarshalIngestList() (error) {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct{
		Count    int
		Next     *string
		Previous *string
		Results  []*Ingest
	}{ 0, nil, nil, nil }
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.ingests = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func(resp *DPNResponse) unmarshalMemberList() (error) {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct{
		Count    int
		Next     *string
		Previous *string
		Results  []*Member
	}{ 0, nil, nil, nil }
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.members = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func(resp *DPNResponse) unmarshalNodeList() (error) {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct{
		Count    int
		Next     *string
		Previous *string
		Results  []*Node
	}{ 0, nil, nil, nil }
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.nodes = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func(resp *DPNResponse) unmarshalReplicationList() (error) {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct{
		Count    int
		Next     *string
		Previous *string
		Results  []*ReplicationTransfer
	}{ 0, nil, nil, nil }
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.replications = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func(resp *DPNResponse) unmarshalRestoreList() (error) {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct{
		Count    int
		Next     *string
		Previous *string
		Results  []*RestoreTransfer
	}{ 0, nil, nil, nil }
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.restores = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}
