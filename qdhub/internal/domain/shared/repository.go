// Package shared provides shared repository interfaces.
package shared

// Repository defines the base repository interface.
type Repository[T any] interface {
	// Create creates a new entity.
	Create(entity *T) error

	// Get retrieves an entity by ID.
	Get(id ID) (*T, error)

	// Update updates an existing entity.
	Update(entity *T) error

	// Delete deletes an entity by ID.
	Delete(id ID) error

	// List retrieves all entities.
	List() ([]*T, error)
}
