// Package auth provides authentication infrastructure implementations.
package auth

import (
	"errors"
	"time"

	"qdhub/internal/domain/shared"

	"github.com/golang-jwt/jwt/v5"
)

// JWTClaims represents JWT token claims.
type JWTClaims struct {
	UserID   string   `json:"user_id"`
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
	jwt.RegisteredClaims
}

// JWTManager handles JWT token generation and validation.
type JWTManager struct {
	secretKey         []byte
	accessExpiration  time.Duration
	refreshExpiration time.Duration
}

// NewJWTManager creates a new JWT manager.
func NewJWTManager(secretKey string, accessExpiration, refreshExpiration time.Duration) *JWTManager {
	return &JWTManager{
		secretKey:         []byte(secretKey),
		accessExpiration:  accessExpiration,
		refreshExpiration: refreshExpiration,
	}
}

// GenerateAccessToken generates an access token for a user.
func (m *JWTManager) GenerateAccessToken(userID shared.ID, username string, roles []string) (string, error) {
	claims := JWTClaims{
		UserID:   userID.String(),
		Username: username,
		Roles:    roles,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(m.accessExpiration)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.secretKey)
}

// GenerateRefreshToken generates a refresh token for a user.
func (m *JWTManager) GenerateRefreshToken(userID shared.ID) (string, error) {
	claims := jwt.RegisteredClaims{
		Subject:   userID.String(),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(m.refreshExpiration)),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		NotBefore: jwt.NewNumericDate(time.Now()),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.secretKey)
}

// ValidateToken validates a JWT token and returns the claims.
func (m *JWTManager) ValidateToken(tokenString string) (*JWTClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return m.secretKey, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*JWTClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, errors.New("invalid token")
}

// ValidateRefreshToken validates a refresh token and returns the user ID.
func (m *JWTManager) ValidateRefreshToken(tokenString string) (shared.ID, error) {
	token, err := jwt.ParseWithClaims(tokenString, &jwt.RegisteredClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return m.secretKey, nil
	})

	if err != nil {
		return "", err
	}

	if claims, ok := token.Claims.(*jwt.RegisteredClaims); ok && token.Valid {
		if claims.Subject == "" {
			return "", errors.New("invalid refresh token: missing subject")
		}
		return shared.ID(claims.Subject), nil
	}

	return "", errors.New("invalid refresh token")
}
