package definition

import "errors"

// RetentionPolicy Defines what to do with old partitions
type RetentionPolicy string

var allPolicies = []RetentionPolicy{N, D, B}

// N = none, D = drop, B = backup
const (
	N = "n"
	D = "d"
	B = "b"
)

func (p RetentionPolicy) toString() string {
	return string(p)
}

// ToRP Convert retention policy from string to RetentionPolicy
func ToRP(rp string) (*RetentionPolicy, error) {
	for _, policy := range allPolicies {
		if policy.toString() == rp {
			return &policy, nil
		}
	}
	return nil, errors.New("invalid retention policy")
}
