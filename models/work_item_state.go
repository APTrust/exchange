package models

import (
	"time"
)

type WorkItemState struct {
	Id              int       `json:"id"`
	WorkItemId      int       `json:"work_item_id"`
	Action          string    `json:"action"`
	State           string    `json:"state"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func NewWorkItemState(workItemId int, action, state string) (*WorkItemState) {
	return &WorkItemState{
		WorkItemId: workItemId,
		Action: action,
		State: state,
	}
}
