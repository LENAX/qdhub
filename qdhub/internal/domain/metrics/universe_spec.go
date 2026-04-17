package metrics

type UniverseSpec struct {
	ExcludeST        *bool `json:"exclude_st,omitempty"`
	ExcludeSuspended *bool `json:"exclude_suspended,omitempty"`
	ExcludeDelisted  *bool `json:"exclude_delisted,omitempty"`
}

func (s *UniverseSpec) Normalize() {
	if s == nil {
		return
	}
	if s.ExcludeST == nil {
		v := true
		s.ExcludeST = &v
	}
	if s.ExcludeSuspended == nil {
		v := true
		s.ExcludeSuspended = &v
	}
	if s.ExcludeDelisted == nil {
		v := true
		s.ExcludeDelisted = &v
	}
}
