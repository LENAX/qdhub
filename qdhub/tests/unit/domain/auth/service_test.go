package auth_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/shared"
)

// MockUserRepository is a mock implementation of UserRepository
type MockUserRepository struct {
	mock.Mock
}

func (m *MockUserRepository) Create(ctx context.Context, user *auth.User) error {
	args := m.Called(ctx, user)
	return args.Error(0)
}

func (m *MockUserRepository) GetByID(ctx context.Context, id shared.ID) (*auth.User, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*auth.User), args.Error(1)
}

func (m *MockUserRepository) GetByUsername(ctx context.Context, username string) (*auth.User, error) {
	args := m.Called(ctx, username)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*auth.User), args.Error(1)
}

func (m *MockUserRepository) GetByEmail(ctx context.Context, email string) (*auth.User, error) {
	args := m.Called(ctx, email)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*auth.User), args.Error(1)
}

func (m *MockUserRepository) Update(ctx context.Context, user *auth.User) error {
	args := m.Called(ctx, user)
	return args.Error(0)
}

func (m *MockUserRepository) Delete(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockUserRepository) List(ctx context.Context, offset, limit int) ([]*auth.User, int64, error) {
	args := m.Called(ctx, offset, limit)
	if args.Get(0) == nil {
		return nil, 0, args.Error(2)
	}
	return args.Get(0).([]*auth.User), args.Get(1).(int64), args.Error(2)
}

// MockPasswordHasher is a mock implementation of PasswordHasher
type MockPasswordHasher struct {
	mock.Mock
}

func (m *MockPasswordHasher) HashPassword(password string) (string, error) {
	args := m.Called(password)
	return args.String(0), args.Error(1)
}

func (m *MockPasswordHasher) VerifyPassword(hashedPassword, password string) bool {
	args := m.Called(hashedPassword, password)
	return args.Bool(0)
}

func TestAuthDomainService_ValidateCredentials(t *testing.T) {
	ctx := context.Background()
	service := auth.NewAuthDomainService()

	t.Run("success", func(t *testing.T) {
		userRepo := new(MockUserRepository)
		hasher := new(MockPasswordHasher)

		user := auth.NewUser("testuser", "test@example.com", "hashed_password")

		userRepo.On("GetByUsername", ctx, "testuser").Return(user, nil)
		hasher.On("VerifyPassword", "hashed_password", "password").Return(true)

		result, err := service.ValidateCredentials(ctx, "testuser", "password", hasher, userRepo)

		assert.NoError(t, err)
		assert.Equal(t, user, result)
		userRepo.AssertExpectations(t)
		hasher.AssertExpectations(t)
	})

	t.Run("user not found", func(t *testing.T) {
		userRepo := new(MockUserRepository)
		hasher := new(MockPasswordHasher)

		userRepo.On("GetByUsername", ctx, "testuser").Return(nil, nil)

		result, err := service.ValidateCredentials(ctx, "testuser", "password", hasher, userRepo)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("user inactive", func(t *testing.T) {
		userRepo := new(MockUserRepository)
		hasher := new(MockPasswordHasher)

		user := auth.NewUser("testuser", "test@example.com", "hashed_password")
		user.UpdateStatus(auth.UserStatusInactive)

		userRepo.On("GetByUsername", ctx, "testuser").Return(user, nil)

		result, err := service.ValidateCredentials(ctx, "testuser", "password", hasher, userRepo)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "not active")
	})

	t.Run("invalid password", func(t *testing.T) {
		userRepo := new(MockUserRepository)
		hasher := new(MockPasswordHasher)

		user := auth.NewUser("testuser", "test@example.com", "hashed_password")

		userRepo.On("GetByUsername", ctx, "testuser").Return(user, nil)
		hasher.On("VerifyPassword", "hashed_password", "wrong_password").Return(false)

		result, err := service.ValidateCredentials(ctx, "testuser", "wrong_password", hasher, userRepo)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "invalid password")
	})
}

func TestBcryptPasswordHasher(t *testing.T) {
	hasher := auth.NewBcryptPasswordHasher(0)

	t.Run("hash and verify", func(t *testing.T) {
		password := "test_password_123"

		hash, err := hasher.HashPassword(password)
		assert.NoError(t, err)
		assert.NotEmpty(t, hash)
		assert.NotEqual(t, password, hash)

		valid := hasher.VerifyPassword(hash, password)
		assert.True(t, valid)

		invalid := hasher.VerifyPassword(hash, "wrong_password")
		assert.False(t, invalid)
	})

	t.Run("different passwords produce different hashes", func(t *testing.T) {
		hash1, _ := hasher.HashPassword("password1")
		hash2, _ := hasher.HashPassword("password2")

		assert.NotEqual(t, hash1, hash2)
	})
}
