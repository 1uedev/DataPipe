package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/1uedev/DataPipe/controlplane/internal/db"
)

// DefaultSessionTTL is how long a login session stays valid.
const DefaultSessionTTL = 24 * time.Hour

// MinPasswordLength is SEC-100's minimal "strong password policy" starting
// point; complexity rules and a breached-password check are deferred (see
// TODO.md).
const MinPasswordLength = 8

type User struct {
	ID           string
	Username     string
	SystemRole   SystemRole
	PasswordHash string
	CreatedAt    time.Time
}

// Store implements local accounts, sessions, and project-role assignment
// on top of the control plane's SQL database.
type Store struct {
	db *db.DB
}

func NewStore(d *db.DB) *Store {
	return &Store{db: d}
}

func ValidatePassword(password string) error {
	if len(password) < MinPasswordLength {
		return fmt.Errorf("auth: password must be at least %d characters", MinPasswordLength)
	}
	return nil
}

// CreateUser hashes password with bcrypt and stores a new local account.
func (s *Store) CreateUser(ctx context.Context, username, password string, systemRole SystemRole) (*User, error) {
	if err := ValidatePassword(password); err != nil {
		return nil, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("auth: hashing password: %w", err)
	}

	u := &User{
		ID:           uuid.NewString(),
		Username:     username,
		SystemRole:   systemRole,
		PasswordHash: string(hash),
		CreatedAt:    time.Now().UTC(),
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO users (id, username, password_hash, system_role, created_at) VALUES (?, ?, ?, ?, ?)`,
		u.ID, u.Username, u.PasswordHash, string(u.SystemRole), u.CreatedAt.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("auth: creating user: %w", err)
	}
	return u, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]*User, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, username, password_hash, system_role, created_at FROM users ORDER BY username`)
	if err != nil {
		return nil, fmt.Errorf("auth: listing users: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var users []*User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func (s *Store) getUserByUsername(ctx context.Context, username string) (*User, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, username, password_hash, system_role, created_at FROM users WHERE username = ?`, username)
	return scanUser(row)
}

func (s *Store) GetUser(ctx context.Context, id string) (*User, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, username, password_hash, system_role, created_at FROM users WHERE id = ?`, id)
	return scanUser(row)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanUser(row rowScanner) (*User, error) {
	var u User
	var systemRole, createdAt string
	if err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &systemRole, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("auth: scanning user: %w", err)
	}
	u.SystemRole = SystemRole(systemRole)
	t, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("auth: parsing created_at: %w", err)
	}
	u.CreatedAt = t
	return &u, nil
}

// Authenticate verifies username/password (SEC-100). Errors are
// intentionally generic (ErrInvalidCredentials) regardless of whether the
// username exists, to avoid username enumeration.
func (s *Store) Authenticate(ctx context.Context, username, password string) (*User, error) {
	u, err := s.getUserByUsername(ctx, username)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrInvalidCredentials
	}
	if err != nil {
		return nil, err
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) != nil {
		return nil, ErrInvalidCredentials
	}
	return u, nil
}

// CreateSession issues a new opaque bearer token for user (a scoped API
// key in spirit, API-120) and returns the raw token — only the token's hash
// is persisted, so a database compromise alone can't be used to log in.
func (s *Store) CreateSession(ctx context.Context, userID string) (token string, expiresAt time.Time, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, fmt.Errorf("auth: generating session token: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(raw)
	expiresAt = time.Now().UTC().Add(DefaultSessionTTL)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO sessions (token_hash, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)`,
		hashToken(token), userID, expiresAt.Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("auth: creating session: %w", err)
	}
	return token, expiresAt, nil
}

// ValidateSession resolves a bearer token to its user, rejecting expired
// sessions.
func (s *Store) ValidateSession(ctx context.Context, token string) (*User, error) {
	var userID, expiresAt string
	row := s.db.QueryRowContext(ctx, `SELECT user_id, expires_at FROM sessions WHERE token_hash = ?`, hashToken(token))
	if err := row.Scan(&userID, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrUnauthorized
		}
		return nil, fmt.Errorf("auth: looking up session: %w", err)
	}
	exp, err := time.Parse(time.RFC3339, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("auth: parsing session expiry: %w", err)
	}
	if time.Now().UTC().After(exp) {
		return nil, ErrUnauthorized
	}
	return s.GetUser(ctx, userID)
}

// RevokeSession deletes a session (logout).
func (s *Store) RevokeSession(ctx context.Context, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash = ?`, hashToken(token))
	return err
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// SetProjectRole assigns (or changes) a user's role on a project. The
// upsert syntax (ON CONFLICT ... DO UPDATE) is supported identically by
// both SQLite (3.24+) and Postgres.
func (s *Store) SetProjectRole(ctx context.Context, projectID, userID string, role ProjectRole) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO project_members (project_id, user_id, role) VALUES (?, ?, ?)
		 ON CONFLICT (project_id, user_id) DO UPDATE SET role = excluded.role`,
		projectID, userID, string(role))
	if err != nil {
		return fmt.Errorf("auth: setting project role: %w", err)
	}
	return nil
}

func (s *Store) RemoveProjectMember(ctx context.Context, projectID, userID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM project_members WHERE project_id = ? AND user_id = ?`, projectID, userID)
	return err
}

// ProjectRoleOf looks up user's role on project; ok is false if they are
// not a member at all.
func (s *Store) ProjectRoleOf(ctx context.Context, userID, projectID string) (role ProjectRole, ok bool, err error) {
	var r string
	row := s.db.QueryRowContext(ctx, `SELECT role FROM project_members WHERE project_id = ? AND user_id = ?`, projectID, userID)
	if err := row.Scan(&r); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("auth: looking up project role: %w", err)
	}
	return ProjectRole(r), true, nil
}

// RequireProjectRole enforces SEC-110: System Admin bypasses project
// scoping entirely; everyone else needs an explicit membership at or above
// min.
func (s *Store) RequireProjectRole(ctx context.Context, user *User, projectID string, min ProjectRole) error {
	if user.SystemRole == SystemRoleAdmin {
		return nil
	}
	role, ok, err := s.ProjectRoleOf(ctx, user.ID, projectID)
	if err != nil {
		return err
	}
	if !ok || !role.AtLeast(min) {
		return ErrForbidden
	}
	return nil
}
