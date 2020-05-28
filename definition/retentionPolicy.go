package definition

import "errors"

type RetentionPolicy string

var allPolicies = []RetentionPolicy{N, D, B}

const (
	N = "n"
	D = "d"
	B = "b"
)

func (p RetentionPolicy) toString() string {
	return string(p)
}

func ToRP(rp string) (*RetentionPolicy, error) {

	for _, policy := range allPolicies {
		if policy.toString() == rp {
			return &policy, nil
		}
	}
	return nil, errors.New("invalid retention policy")
}
