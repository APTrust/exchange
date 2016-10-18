package dpn

// SyncResult describes the result of an operation where we pull
// info about all updated bags, replication requests and restore
// requests from a remote node and copy that data into our own
// local DPN registry.
type SyncResult struct {
	NodeName       string
	FetchCounts    map[DPNObjectType]int
	SyncCounts     map[DPNObjectType]int
	Errors         map[DPNObjectType][]error
}

// NewSyncResult creates a new SyncResult.
func NewSyncResult(nodeName string) (*SyncResult) {
	return &SyncResult{
		NodeName: nodeName,
		FetchCounts: make(map[DPNObjectType]int),
		SyncCounts: make(map[DPNObjectType]int),
		Errors: make(map[DPNObjectType][]error),
	}
}

// AddToFetchCount adds increment to the specified objectType count,
// where objectType is the type of object fetched (bag, fixity check,
// etc.)
func (syncResult *SyncResult) AddToFetchCount (objectType DPNObjectType, increment int) {
	if _, keyExists := syncResult.FetchCounts[objectType]; !keyExists {
		syncResult.FetchCounts[objectType] = 0
	}
	syncResult.FetchCounts[objectType] += increment
}

// AddToSyncCount adds increment to the specified objectType count,
// where objectType is the type of object fetched (bag, fixity check,
// etc.)
func (syncResult *SyncResult) AddToSyncCount (objectType DPNObjectType, increment int) {
	if _, keyExists := syncResult.SyncCounts[objectType]; !keyExists {
		syncResult.SyncCounts[objectType] = 0
	}
	syncResult.SyncCounts[objectType] += increment
}

// AddError adds an error for the specified objectType (bag, replication, etc.)
func (syncResult *SyncResult) AddError (objectType DPNObjectType, err error) {
	if _, keyExists := syncResult.Errors[objectType]; !keyExists {
		syncResult.Errors[objectType] = make([]error, 0)
	}
	syncResult.Errors[objectType] = append(syncResult.Errors[objectType], error)
}

// HasErrors returns true if there are any errors for the specified objectType.
// If objectType is nil, this will check for errors in all object types
func (syncResult *SyncResult) HasErrors(objectType DPNObjectType) (bool) {
	hasErrors := false
	if objectType == "" {
		for objType, errors := range syncResult.Errors {
			if len(errors) > 0 {
				hasErrors = true
				break
			}
		}
	} else {
		if errors, keyExists := syncResult.Errors[objectType]; keyExists {
			hasErrors = len(errors) > 0
		}
	}
	return hasErrors
}
