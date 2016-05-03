package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// PharosClient supports basic calls to the Pharos Admin REST API.
// This client does not support the Member API.
type PharosClient struct {
	hostUrl      string
	apiVersion   string
	apiUser      string
	apiKey       string
	httpClient   *http.Client
	transport    *http.Transport
}

// Creates a new pharos client. Param hostUrl should come from
// the config.json file.
func NewPharosClient(hostUrl, apiVersion, apiUser, apiKey string) (*PharosClient, error) {
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
	return &PharosClient{
		hostUrl: hostUrl,
		apiVersion: apiVersion,
		apiUser: apiUser,
		apiKey: apiKey,
		httpClient: httpClient,
		transport: transport}, nil
}

// Returns a list of depositing member institutions.
func (client *PharosClient) InstitutionGet(identifier string) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosInstitution)
	resp.institutions = make([]*models.Institution, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/institutions/%s", client.apiVersion, escapeSlashes(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	institution := &models.Institution{}
	resp.Error = json.Unmarshal(resp.data, institution)
	if resp.Error == nil {
		resp.institutions[0] = institution
	}
	return resp
}


// Returns a list of APTrust depositor institutions.
func (client *PharosClient) InstitutionList() (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosInstitution)
	resp.institutions = make([]*models.Institution, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/institutions/", client.apiVersion)
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}


// Returns the object with the specified identifier, if it
// exists. Param identifier is an IntellectualObject identifier
// in the format "institution.edu/object_name"
func (client *PharosClient) IntellectualObjectGet(identifier string) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosIntellectualObject)
	resp.objects = make([]*models.IntellectualObject, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/objects/%s", client.apiVersion, escapeSlashes(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	intelObj := &models.IntellectualObject{}
	resp.Error = json.Unmarshal(resp.data, intelObj)
	if resp.Error == nil {
		resp.objects[0] = intelObj
	}
	return resp
}

// Returns a list of IntellectualObjects matching the filter criteria
// specified in params. Params include:
//
// * institution - Return objects belonging to this institution.
// * updated_since - Return object updated since this date.
// * name_contains - Return objects whose name contains the specified string.
// * name_exact - Return only object with the exact name specified.
// * state = 'A' for active records, 'D' for deleted. Default is 'A'
func (client *PharosClient) IntellectualObjectList(params map[string]string) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosIntellectualObject)
	resp.objects = make([]*models.IntellectualObject, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/objects/?%s", client.apiVersion, BuildQueryString(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}


	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// Saves the intellectual object to Pharos. If the object has an ID of zero,
// this performs a POST to create a new Intellectual Object. If the ID is
// non-zero, this updates the existing object with a PUT. The response object
// will contain a new copy of the IntellectualObject if it was successfully
// saved.
func (client *PharosClient) IntellectualObjectSave(obj *models.IntellectualObject) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosIntellectualObject)
	resp.objects = make([]*models.IntellectualObject, 1)

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/objects", client.apiVersion)
	httpMethod := "POST"
	if obj.Id > 0 {
		relativeUrl = fmt.Sprintf("%s/%s", relativeUrl, escapeSlashes(obj.Identifier))
		httpMethod = "PUT"
	}
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := obj.SerializeForPharos()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	intelObj := &models.IntellectualObject{}
	resp.Error = json.Unmarshal(resp.data, intelObj)
	if resp.Error == nil {
		resp.objects[0] = intelObj
	}
	return resp
}

// Returns the GenericFile having the specified identifier. The identifier
// should be in the format "institution.edu/object_name/path/to/file.ext"
func (client *PharosClient) GenericFileGet(identifier string) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosGenericFile)
	resp.files = make([]*models.GenericFile, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/files/%s", client.apiVersion, escapeSlashes(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	gf := &models.GenericFile{}
	resp.Error = json.Unmarshal(resp.data, gf)
	if resp.Error == nil {
		resp.files[0] = gf
	}
	return resp
}

// Returns a list of Generic Files. Params include:
//
// * intellectual_object_identifier - The identifier of the object to which
//   the files belong.
func (client *PharosClient) GenericFileList(params map[string]string) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosGenericFile)
	resp.files = make([]*models.GenericFile, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/files/?%s", client.apiVersion, BuildQueryString(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// Saves a Generic File record to Pharos. If the Generic File's ID is zero,
// this performs a POST to create a new record. For non-zero IDs, this
// performs a PUT to update the existing record. Either way, the record
// must have an IntellectualObject ID. The response object will have a new
// copy of the GenericFile if the save was successful.
func (client *PharosClient) GenericFileSave(obj *models.GenericFile) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosGenericFile)
	resp.files = make([]*models.GenericFile, 1)

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/files", client.apiVersion)
	httpMethod := "POST"
	if obj.Id > 0 {
		relativeUrl = fmt.Sprintf("%s/%s", relativeUrl, escapeSlashes(obj.Identifier))
		httpMethod = "PUT"
	}
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := obj.SerializeForPharos()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	gf := &models.GenericFile{}
	resp.Error = json.Unmarshal(resp.data, gf)
	if resp.Error == nil {
		resp.files[0] = gf
	}
	return resp
}

// Returns the PREMIS event with the specified identifier. The identifier
// should be a UUID in string format, with dashes. E.g.
// "49a7d6b5-cdc1-4912-812e-885c08e90c68"
func (client *PharosClient) PremisEventGet(identifier string) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosPremisEvent)
	resp.events = make([]*models.PremisEvent, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/events/%s", client.apiVersion, escapeSlashes(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	event := &models.PremisEvent{}
	resp.Error = json.Unmarshal(resp.data, event)
	if resp.Error == nil {
		resp.events[0] = event
	}
	return resp
}

// Returns a list of PREMIS events matching the specified criteria.
// Parameters include:
//
// * intellectual_object_identifier - (string) Return events associated with
//   the specified intellectual object (but not its generic files).
// * generic_file_identifier - (string) Return events associated with the
//   specified generic file.
// * event_type - (string) Return events of the specified type. See the
//   event types listed in contants/constants.go
// * created_since - (iso 8601 datetime string) Return events created
//   on or after the specified datetime.
func (client *PharosClient) PremisEventList(params map[string]string) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosPremisEvent)
	resp.events = make([]*models.PremisEvent, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/events/?%s", client.apiVersion, BuildQueryString(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// Saves a PREMIS event to Pharos. If the event ID is zero, this issues a
// POST request to create a new event record. If the ID is non-zero, this
// issues a PUT to update the existing event. The response object will
// have a new copy of the Premis event if the save was successful.
func (client *PharosClient) PremisEventSave(obj *models.PremisEvent) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosPremisEvent)
	resp.events = make([]*models.PremisEvent, 1)

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/events", client.apiVersion)
	httpMethod := "POST"
	if obj.Id > 0 {
		relativeUrl = fmt.Sprintf("%s/%s", relativeUrl, escapeSlashes(obj.Identifier))
		httpMethod = "PUT"
	}
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := json.Marshal(obj)
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	event := &models.PremisEvent{}
	resp.Error = json.Unmarshal(resp.data, event)
	if resp.Error == nil {
		resp.events[0] = event
	}
	return resp
}

// Lists the work items meeting the specified filters, or all work
// items if no filter params are set. Params include:
//
// * name
// * etag
// * bag_date
// * stage
// * status
// * institution,
// * retry
// * reviewed
// * object_identifier
// * generic_file_identifier
// * node
// * needs_admin_review
// * process_after
func (client *PharosClient) WorkItemList(params map[string]string) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*models.WorkItem, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/work_items/?%s", client.apiVersion, BuildQueryString(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// Saves a WorkItem record to Pharos. If the WorkItems's ID is zero,
// this performs a POST to create a new record. For non-zero IDs, this
// performs a PUT to update the existing record. The response object
// will include a new copy of the WorkItem if it was saved successfully.
func (client *PharosClient) WorkItemSave(obj *models.GenericFile) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*models.WorkItem, 1)

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/work_items", client.apiVersion)
	httpMethod := "POST"
	if obj.Id > 0 {
		relativeUrl = fmt.Sprintf("%s/%d", relativeUrl, obj.Id)
		httpMethod = "PUT"
	}
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := obj.SerializeForPharos()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	workItem := &models.WorkItem{}
	resp.Error = json.Unmarshal(resp.data, workItem)
	if resp.Error == nil {
		resp.workItems[0] = workItem
	}
	return resp
}

// Returns the WorkItem with the specified ID.
func (client *PharosClient) WorkItemGet(id int) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*models.WorkItem, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/work_items/%d/", client.apiVersion, id)
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	workItem := &models.WorkItem{}
	resp.Error = json.Unmarshal(resp.data, workItem)
	if resp.Error == nil {
		resp.workItems[0] = workItem
	}
	return resp
}

// Returns the WorkItem with the specified etag, name and bag_date.
func (client *PharosClient) WorkItemGetByEtagNameBagDate(etag, name string, bagDate time.Time) (*PharosResponse) {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*models.WorkItem, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/work_items/%s/%s/%s", client.apiVersion, etag, name, bagDate)
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	workItem := &models.WorkItem{}
	resp.Error = json.Unmarshal(resp.data, workItem)
	if resp.Error == nil {
		resp.workItems[0] = workItem
	}
	return resp
}


// -------------------------------------------------------------------------
// Utility Methods
// -------------------------------------------------------------------------


// BuildUrl combines the host and protocol in client.hostUrl with
// relativeUrl to create an absolute URL. For example, if client.hostUrl
// is "http://localhost:3456", then client.BuildUrl("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *PharosClient) BuildUrl(relativeUrl string) string {
	return client.hostUrl + relativeUrl
}

// NewJsonRequest returns a new request with headers indicating
// JSON request and response formats.
//
// Param method can be "GET", "POST", or "PUT". The Pharos service
// currently only supports those three.
//
// Param absoluteUrl should be the absolute URL. For get requests,
// include params in the query string rather than in the
// requestData param.
//
// Param requestData will be nil for GET requests, and can be
// constructed from bytes.NewBuffer([]byte) for POST and PUT.
// For the PharosClient, we're typically sending JSON data in
// the request body.
func (client *PharosClient) NewJsonRequest(method, absoluteUrl string, requestData io.Reader) (*http.Request, error){
	req, err := http.NewRequest(method, absoluteUrl, requestData)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Pharos-API-User", client.apiUser)
	req.Header.Add("X-Pharos-API-Key", client.apiKey)
	req.Header.Add("Connection", "Keep-Alive")

	// Unfix the URL that golang net/url "fixes" for us.
	// URLs that contain %2F (encoded slashes) MUST preserve
	// the %2F. The Go URL library silently converts those
	// to slashes, and we DON'T want that!
	// See http://stackoverflow.com/questions/20847357/golang-http-client-always-escaped-the-url/
    incorrectUrl, err := url.Parse(absoluteUrl)
    if err != nil {
        return nil, err
    }
    opaqueUrl := strings.Replace(absoluteUrl, client.hostUrl, "", 1)

    // This fixes an issue with GenericFile names that include spaces.
    opaqueUrl = strings.Replace(opaqueUrl, " ", "%20", -1)

	correctUrl := &url.URL{
		Scheme: incorrectUrl.Scheme,
		Host:   incorrectUrl.Host,
		Opaque: opaqueUrl,
	}
	req.URL = correctUrl
	return req, nil
}

// DoRequest issues an HTTP request, reads the response, and closes the
// connection to the remote server.
//
// Param resp should be a PharosResponse.
//
// For a description of the other params, see NewJsonRequest.
//
// If an error occurs, it will be recorded in resp.Error.
func (client *PharosClient) DoRequest(resp *PharosResponse, method, absoluteUrl string, requestData io.Reader) {
	// Build the request
	request, err := client.NewJsonRequest("GET", absoluteUrl, nil)
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

// Converts a set map of query params into a URL-encoded string.
func BuildQueryString(params map[string]string) (string) {
	if params == nil {
		return ""
	}
	values := url.Values{}
	for key, value := range params {
		values.Add(key, value)
	}
	return values.Encode()
}

// Replaces "/" with "%2F", which golang's url.QueryEscape does not do.
func escapeSlashes(s string) string {
	return strings.Replace(s, "/", "%2F", -1)
}