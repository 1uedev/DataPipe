// Package auth implements local-account authentication and RBAC (SEC-100,
// SEC-110): password accounts with bcrypt hashing, opaque bearer session
// tokens, and project-scoped roles (System Admin bypasses project scoping
// entirely — "System Admin" per SEC-110's role list).
package auth

import "errors"

// SystemRole is a platform-wide role, independent of any project.
type SystemRole string

const (
	SystemRoleNone  SystemRole = "none"
	SystemRoleAdmin SystemRole = "system_admin"
)

// ProjectRole is SEC-110's project-scoped role, ordered least to most
// privileged: Viewer < Operator < Editor < Project Admin.
type ProjectRole string

const (
	RoleViewer       ProjectRole = "viewer"
	RoleOperator     ProjectRole = "operator"
	RoleEditor       ProjectRole = "editor"
	RoleProjectAdmin ProjectRole = "project_admin"
)

var roleRank = map[ProjectRole]int{
	RoleViewer:       0,
	RoleOperator:     1,
	RoleEditor:       2,
	RoleProjectAdmin: 3,
}

// AtLeast reports whether r has at least the privilege of min. An
// unrecognized role ranks below everything.
func (r ProjectRole) AtLeast(min ProjectRole) bool {
	return roleRank[r] >= roleRank[min]
}

// ErrForbidden is returned by authorization checks; API handlers map it to
// HTTP 403.
var ErrForbidden = errors.New("auth: forbidden")

// ErrInvalidCredentials is returned by Authenticate on a bad username or
// password (deliberately not distinguished, to avoid username enumeration).
var ErrInvalidCredentials = errors.New("auth: invalid credentials")

// ErrUnauthorized is returned when no valid session is presented.
var ErrUnauthorized = errors.New("auth: unauthorized")
