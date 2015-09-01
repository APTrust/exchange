package result

// All of the result types in this package implement this interface.
type Result interface {
	Started() bool
	Succeeded() bool
	AddError(string)
}
