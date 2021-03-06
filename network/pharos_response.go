package network

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"io/ioutil"
	"net/http"
	"net/url"
)

type PharosResponse struct {
	// Count is the total number of items matching the
	// specified filters. This is useful for List requests.
	// Note that the number of items returned in the response
	// may be fewer than ItemCount. For example, the remote
	// server may return only 10 of 10,000 matching records
	// at a time.
	Count int

	// The URL of the next page of results.
	Next *string

	// The URL of the next page of results.
	Previous *string

	// The HTTP request that was (or would have been) sent to
	// the Pharos REST server. This is useful for logging and
	// debugging.
	Request *http.Request

	// The HTTP Response from the server. You can get the
	// HTTP status code, headers, etc. through this. See
	// https://golang.org/pkg/net/http/#Response for more info.
	//
	// Do not try to read Response.Body, since it's already been read
	// and the stream has been closed. Use the RawResponseData()
	// method instead.
	Response *http.Response

	// The error, if any, that occurred while processing this
	// request. Errors may come from the server (4xx or 5xx
	// responses) or from the client (e.g. if it could not
	// parse the JSON response).
	Error error

	// The type of object(s) this response contains.
	objectType PharosObjectType

	// A slice of IntellectualObject pointers. Will be nil if
	// objectType is not IntellectualObject.
	objects []*models.IntellectualObject

	// A slice of GenericFile pointers. Will be nil if
	// objectType is not GenericFile.
	files []*models.GenericFile

	// A slice of Checksum pointers. Will be nil if
	// objectType is not Checksum.
	checksums []*models.Checksum

	// A slice of PremisEvent pointers. Will be nil if
	// objectType is not PremisEvent.
	events []*models.PremisEvent

	// A slice of Institution pointers. Will be nil if
	// objectType is not Institution.
	institutions []*models.Institution

	// A slice of WorkItem pointers. Will be nil if
	// objectType is not WorkItem.
	workItems []*models.WorkItem

	// A slice of WorkItemState pointers. Will be nil if
	// objectType is not WorkItemState.
	workItemStates []*models.WorkItemState

	// Indicates whether the HTTP response body has been
	// read (and closed).
	hasBeenRead bool

	listHasBeenParsed bool

	// The raw data contained in the body of the HTTP
	// respone.
	data []byte
}

type PharosObjectType string

const (
	PharosIntellectualObject PharosObjectType = "IntellectualObject"
	PharosInstitution                         = "Institution"
	PharosGenericFile                         = "GenericFile"
	PharosChecksum                            = "Checksum"
	PharosPremisEvent                         = "PremisEvent"
	PharosWorkItem                            = "WorkItem"
	PharosWorkItemState                       = "WorkItemState"
)

// Creates a new PharosResponse and returns a pointer to it.
func NewPharosResponse(objType PharosObjectType) *PharosResponse {
	return &PharosResponse{
		Count:             0,
		Next:              nil,
		Previous:          nil,
		objectType:        objType,
		hasBeenRead:       false,
		listHasBeenParsed: false,
	}
}

// Returns the raw body of the HTTP response as a byte slice.
// The return value may be nil.
func (resp *PharosResponse) RawResponseData() ([]byte, error) {
	if !resp.hasBeenRead {
		resp.readResponse()
	}
	return resp.data, resp.Error
}

// Reads the body of an HTTP response object, closes the stream, and
// returns a byte array. The body MUST be closed, or you'll wind up
// with a lot of open network connections.
func (resp *PharosResponse) readResponse() {
	if !resp.hasBeenRead && resp.Response != nil && resp.Response.Body != nil {
		resp.data, resp.Error = ioutil.ReadAll(resp.Response.Body)
		resp.Response.Body.Close()
		resp.hasBeenRead = true
	}
}

// Returns the type of object(s) contained in this response.
func (resp *PharosResponse) ObjectType() PharosObjectType {
	return resp.objectType
}

// Returns true if the response includes a link to the next page
// of results.
func (resp *PharosResponse) HasNextPage() bool {
	return resp.Next != nil && *resp.Next != ""
}

// Returns true if the response includes a link to the previous page
// of results.
func (resp *PharosResponse) HasPreviousPage() bool {
	return resp.Previous != nil && *resp.Previous != ""
}

// Returns the URL parameters to request the next page of results,
// or nil if there is no next page.
func (resp *PharosResponse) ParamsForNextPage() url.Values {
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
func (resp *PharosResponse) ParamsForPreviousPage() url.Values {
	if resp.HasPreviousPage() {
		previousUrl, _ := url.Parse(*resp.Previous)
		if previousUrl != nil {
			return previousUrl.Query()
		}
	}
	return nil
}

// Returns the Institution parsed from the HTTP response body, or nil.
func (resp *PharosResponse) Institution() *models.Institution {
	if resp.institutions != nil && len(resp.institutions) > 0 {
		return resp.institutions[0]
	}
	return nil
}

// Returns a list of Institutions parsed from the HTTP response body.
func (resp *PharosResponse) Institutions() []*models.Institution {
	if resp.institutions == nil {
		return make([]*models.Institution, 0)
	}
	return resp.institutions
}

// Returns the IntellectualObject parsed from the HTTP response body,
// or nil.
func (resp *PharosResponse) IntellectualObject() *models.IntellectualObject {
	if resp.objects != nil && len(resp.objects) > 0 {
		return resp.objects[0]
	}
	return nil
}

// Returns a list of IntellectualObjects parsed from the HTTP response body.
func (resp *PharosResponse) IntellectualObjects() []*models.IntellectualObject {
	if resp.objects == nil {
		return make([]*models.IntellectualObject, 0)
	}
	return resp.objects
}

// Returns the GenericFile parsed from the HTTP response body,  or nil.
func (resp *PharosResponse) GenericFile() *models.GenericFile {
	if resp.files != nil && len(resp.files) > 0 {
		return resp.files[0]
	}
	return nil
}

// Returns a list of GenericFiles parsed from the HTTP response body.
func (resp *PharosResponse) GenericFiles() []*models.GenericFile {
	if resp.files == nil {
		return make([]*models.GenericFile, 0)
	}
	return resp.files
}

// Returns the Checksum parsed from the HTTP response body,  or nil.
func (resp *PharosResponse) Checksum() *models.Checksum {
	if resp.checksums != nil && len(resp.checksums) > 0 {
		return resp.checksums[0]
	}
	return nil
}

// Returns a list of Checksums parsed from the HTTP response body.
func (resp *PharosResponse) Checksums() []*models.Checksum {
	if resp.checksums == nil {
		return make([]*models.Checksum, 0)
	}
	return resp.checksums
}

// Returns the PremisEvent parsed from the HTTP response body, or nil.
func (resp *PharosResponse) PremisEvent() *models.PremisEvent {
	if resp.events != nil && len(resp.events) > 0 {
		return resp.events[0]
	}
	return nil
}

// Returns a list of PremisEvents parsed from the HTTP response body.
func (resp *PharosResponse) PremisEvents() []*models.PremisEvent {
	if resp.events == nil {
		return make([]*models.PremisEvent, 0)
	}
	return resp.events
}

// Returns the WorkItem parsed from the HTTP response body, or nil.
func (resp *PharosResponse) WorkItem() *models.WorkItem {
	if resp.workItems != nil && len(resp.workItems) > 0 {
		return resp.workItems[0]
	}
	return nil
}

// Returns a list of WorkItems parsed from the HTTP response body.
func (resp *PharosResponse) WorkItems() []*models.WorkItem {
	if resp.workItems == nil {
		return make([]*models.WorkItem, 0)
	}
	return resp.workItems
}

// Returns the WorkItemState parsed from the HTTP response body, or nil.
func (resp *PharosResponse) WorkItemState() *models.WorkItemState {
	if resp.workItemStates != nil && len(resp.workItemStates) > 0 {
		return resp.workItemStates[0]
	}
	return nil
}

// Returns a list of WorkItemStates parsed from the HTTP response body.
func (resp *PharosResponse) WorkItemStates() []*models.WorkItemState {
	if resp.workItemStates == nil {
		return make([]*models.WorkItemState, 0)
	}
	return resp.workItemStates
}

// UnmarshalJsonList converts JSON response from the Pharos server
// into a list of usable objects. The Pharos list response has this
// structure:
//
// {
//   "count": 500
//   "next": "https://example.com/objects/per_page=20&page=11"
//   "previous": "https://example.com/objects/per_page=20&page=9"
//   "results": [... array of arbitrary objects ...]
// }
func (resp *PharosResponse) UnmarshalJsonList() error {
	switch resp.objectType {
	case PharosIntellectualObject:
		return resp.decodeAsObjectList()
	case PharosInstitution:
		return resp.decodeAsInstitutionList()
	case PharosGenericFile:
		return resp.decodeAsGenericFileList()
	case PharosChecksum:
		return resp.decodeAsChecksumList()
	case PharosPremisEvent:
		return resp.decodeAsPremisEventList()
	case PharosWorkItem:
		return resp.decodeAsWorkItemList()
	case PharosWorkItemState:
		return resp.decodeAsWorkItemStateList()
	default:
		return fmt.Errorf("PharosObjectType %v not supported", resp.objectType)
	}
}

func (resp *PharosResponse) decodeAsObjectList() error {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct {
		Count    int                          `json:"count"`
		Next     *string                      `json:"next"`
		Previous *string                      `json:"previous"`
		Results  []*models.IntellectualObject `json:"results"`
	}{0, nil, nil, nil}
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.objects = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func (resp *PharosResponse) decodeAsInstitutionList() error {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct {
		Count    int                   `json:"count"`
		Next     *string               `json:"next"`
		Previous *string               `json:"previous"`
		Results  []*models.Institution `json:"results"`
	}{0, nil, nil, nil}
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.institutions = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func (resp *PharosResponse) decodeAsGenericFileList() error {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct {
		Count    int                   `json:"count"`
		Next     *string               `json:"next"`
		Previous *string               `json:"previous"`
		Results  []*models.GenericFile `json:"results"`
	}{0, nil, nil, nil}
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.files = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func (resp *PharosResponse) decodeAsChecksumList() error {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct {
		Count    int                `json:"count"`
		Next     *string            `json:"next"`
		Previous *string            `json:"previous"`
		Results  []*models.Checksum `json:"results"`
	}{0, nil, nil, nil}
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.checksums = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func (resp *PharosResponse) decodeAsPremisEventList() error {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct {
		Count    int                   `json:"count"`
		Next     *string               `json:"next"`
		Previous *string               `json:"previous"`
		Results  []*models.PremisEvent `json:"results"`
	}{0, nil, nil, nil}
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.events = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func (resp *PharosResponse) decodeAsWorkItemList() error {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct {
		Count    int                `json:"count"`
		Next     *string            `json:"next"`
		Previous *string            `json:"previous"`
		Results  []*models.WorkItem `json:"results"`
	}{0, nil, nil, nil}
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.workItems = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}

func (resp *PharosResponse) decodeAsWorkItemStateList() error {
	if resp.listHasBeenParsed {
		return nil
	}
	temp := struct {
		Count    int                     `json:"count"`
		Next     *string                 `json:"next"`
		Previous *string                 `json:"previous"`
		Results  []*models.WorkItemState `json:"results"`
	}{0, nil, nil, nil}
	data, err := resp.RawResponseData()
	if err != nil {
		resp.Error = err
		return err
	}
	resp.Error = json.Unmarshal(data, &temp)
	resp.Count = temp.Count
	resp.Next = temp.Next
	resp.Previous = temp.Previous
	resp.workItemStates = temp.Results
	resp.listHasBeenParsed = true
	return resp.Error
}
