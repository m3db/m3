package auth

// CredentialType designates credentials for different connection edges.
type CredentialType int

const (
	// Unknown defines unknown connection edge.
	Unknown CredentialType = iota

	// ClientCredential defines m3db client to dbnode connection credentials.
	ClientCredential

	// PeerCredential defines dbnode to dbnode connections credentials.
	PeerCredential

	// EtcdCredential defines dbnode to etcd connections credentials.
	EtcdCredential
)

// Mode designates a type of authentication.
type Mode int

const (
	// AuthModeUnknown is unknown authentication type case.
	AuthModeUnknown Mode = iota

	// AuthModeNoAuth is no authentication type case.
	AuthModeNoAuth

	// AuthModeShadow mode runs authentication in shadow mode. Credentials will be passed
	// by respective peers/clients but will not be used to reject RPCs in case of auth failure.
	AuthModeShadow

	// AuthModeEnforced mode runs dbnode in enforced authentication mode. RPCs to dbnode will be rejected
	// if auth fails.
	AuthModeEnforced
)
