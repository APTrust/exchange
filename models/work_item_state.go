package models

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"time"
)

type WorkItemState struct {
	Id         int       `json:"id"`
	WorkItemId int       `json:"work_item_id"`
	Action     string    `json:"action"`
	State      string    `json:"state"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
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
	ingestManifest := &IngestManifest{}
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
