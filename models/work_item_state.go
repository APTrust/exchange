package models

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"time"
)

// WorkItemState contains information about what work has been completed,
// and what work remains to be done, for the associated WorkItem. WorkItems
// that have not yet started processing will have no associated WorkItemState.
type WorkItemState struct {
	// Id is the unique identifier for this WorkItemState object.
	Id int `json:"id"`
	// WorkItemId is the unique identifier of the WorkItem whose state
	// this object describes.
	WorkItemId int `json:"work_item_id"`
	// Action is the WorkItem action to be performed. See constants.ActionTypes.
	Action string `json:"action"`
	// State is a JSON string describing the state of processing, what work has
	// been completed, and what work remains to be done. This JSON string
	// deserializes to different types, based on the Action. For example, if
	// Action is "Ingest", the JSON deserializes to in IngestState object.
	// The workers in the /workers directory retrieve this state when they begin
	// work an a WorkItem and update it when they stop work on that item.
	//
	// It's common for long-running tasks to fail due to network errors. For
	// example, copying 10,000 files from a large bag to S3 and/or Glacier
	// may fail half-way through due to connectivity problems. When that
	// happens, the worker will stop work on the item and preserve its state
	// in JSON format in this field. The next worker to pick up the task will
	// deserialize the JSON state info, see that the first 5000 files were
	// already successfully stored, and then resume the storage work at file
	// #5001.
	//
	// This state information is essential for intelligently resuming work
	// after failures, and for forensics on failed items. Admin users can
	// see the state JSON in the WorkItem detail view in Pharos.
	State string `json:"state"`
	// CreatedAt is the Rails timestamp describing when this item was created.
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt is the Rails timestamp describing when this item was updated.
	UpdatedAt time.Time `json:"updated_at"`
}

func NewWorkItemState(workItemId int, action, state string) *WorkItemState {
	return &WorkItemState{
		WorkItemId: workItemId,
		Action:     action,
		State:      state,
	}
}

func (state *WorkItemState) HasData() bool {
	return state.State != ""
}

// IngestManifest converts the State string (JSON) to an IngestManifest
// object. This works only if there's data in the State string, and the
// Action is constants.ActionIngest. Other actions will have different
// types of data in the State string.
func (state *WorkItemState) IngestManifest() (*IngestManifest, error) {
	if !state.HasData() {
		return nil, fmt.Errorf("Cannot convert state to IngestManifest because state is empty.")
	}
	if state.Action != constants.ActionIngest {
		return nil, fmt.Errorf("Cannot convert state to IngestManifest because action is '%s' "+
			"and must be '%s'.", state.Action, constants.ActionIngest)
	}
	ingestManifest := NewIngestManifest()
	err := json.Unmarshal([]byte(state.State), ingestManifest)
	return ingestManifest, err
}

// Converts an IngestManifest into a JSON string and stores it in the State
// attribute.
func (state *WorkItemState) SetStateFromIngestManifest(manifest *IngestManifest) error {
	if state.Action != constants.ActionIngest {
		return fmt.Errorf("Cannot set state from IngestManifest because action is '%s' "+
			"and must be '%s'.", state.Action, constants.ActionIngest)
	}
	jsonData, err := json.MarshalIndent(manifest, "", "  ")
	if err == nil {
		state.State = string(jsonData)
	}
	return err
}

func (state *WorkItemState) GlacierRestoreState() (*GlacierRestoreState, error) {
	if !state.HasData() {
		return nil, fmt.Errorf("Cannot convert state to WorkSummary because state is empty.")
	}
	if state.Action != constants.ActionGlacierRestore {
		return nil, fmt.Errorf("Cannot convert state to WorkSummary because action is '%s' "+
			"and must be '%s'.", state.Action, constants.ActionGlacierRestore)
	}
	glacierRestoreState := &GlacierRestoreState{}
	err := json.Unmarshal([]byte(state.State), glacierRestoreState)
	return glacierRestoreState, err
}
