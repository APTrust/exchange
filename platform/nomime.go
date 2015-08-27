// +build partners

// Stub function for GuessMimeType. This is for partner-apps, where
// the function is never actually called. We need the function to be
// defined, or our build will fail.
//
// GuessMimeType is not used in partner apps because it relies on
// external C libraries that partners probably will not have on their
// machines.
package platform

var IsPartnerBuild = true

func GuessMimeType(absPath string) (mimeType string, err error) {
	return "mime type disabled", nil
}
