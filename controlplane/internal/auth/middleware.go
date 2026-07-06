package auth

import (
	"context"
	"net/http"
	"strings"
)

type contextKey int

const userContextKey contextKey = 0

// Middleware requires a valid "Authorization: Bearer <token>" session and
// attaches the resolved User to the request context; everything downstream
// of this API is authenticated (ARC-110: everything the UI can do goes
// through this API).
func (s *Store) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		user, err := s.ValidateSession(r.Context(), token)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userContextKey, user)))
	})
}

// UserFromContext retrieves the User attached by Middleware.
func UserFromContext(ctx context.Context) (*User, bool) {
	u, ok := ctx.Value(userContextKey).(*User)
	return u, ok
}
