// @2025 QSAN Inc. All rights reserved

package goqsan

import (
	"context"
	"net/http"
)

// AccountOp handles account related methods of the QSAN storage.
type AccountOp struct {
	client *AuthClient
}

type ListUsersResponse struct {
	Users []UserData `json:"users"`
	Total int        `json:"total"`
}

type UserData struct {
	Name           string `json:"name"`
	ID             string `json:"id"`
	DomainID       string `json:"domainId"`
	Description    string `json:"description"`
	Email          string `json:"email"`
	IsAdmin        bool   `json:"isAdmin"`
	IsDisable      bool   `json:"isDisable"`
	IsDefault      bool   `json:"isDefault"`
	EnableUserhome bool   `json:"enableUserhome"`
	Enable2FA      bool   `json:"enable2FA"`
}

type ListGroupsResponse struct {
	Groups []GroupData `json:"groups"`
	Total  int         `json:"total"`
}

type GroupData struct {
	Name        string `json:"name"`
	ID          string `json:"id"`
	DomainID    string `json:"domainId"`
	Description string `json:"description"`
	IsDefault   bool   `json:"isDefault"`
}

func NewAccount(client *AuthClient) *AccountOp {
	return &AccountOp{client}
}

// ListUsers list all users
func (v *AccountOp) ListUsers(ctx context.Context) (*[]UserData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/account/domains/local/users", nil)
	if err != nil {
		return nil, err
	}

	var res ListUsersResponse
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res.Users, nil
}

// ListUserByID list a dedicated user using userId
func (v *AccountOp) ListUserByID(ctx context.Context, userId string) (*UserData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/account/domains/local/users/"+userId, nil)
	if err != nil {
		return nil, err
	}

	res := UserData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListGroups list all groups
func (v *AccountOp) ListGroups(ctx context.Context) (*[]GroupData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/account/domains/local/groups", nil)
	if err != nil {
		return nil, err
	}

	var res ListGroupsResponse
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res.Groups, nil
}

// ListGroupByID list a dedicated group using groupId
func (v *AccountOp) ListGroupByID(ctx context.Context, groupId string) (*GroupData, error) {

	req, err := v.client.NewRequest(ctx, http.MethodGet, "/rest/v2/account/domains/local/groups/"+groupId, nil)
	if err != nil {
		return nil, err
	}

	res := GroupData{}
	if err := v.client.SendRequest(ctx, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
