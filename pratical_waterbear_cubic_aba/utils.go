package pratical_waterbear_cubic_aba

import (
	"math/rand"
	"sync"
	"time"
)

type NodeId string
type Origin NodeId
type Sender NodeId
type Prover NodeId
type Witness NodeId
type Target NodeId
type SessionId string
type Val int
type BSet int
type Round int
type Stage string
type RBCType string
type Total int

const (
	ValUnknown Val = 3
	Val0       Val = 0
	Val1       Val = 1
	ValStar    Val = 2
)

const (
	BSetEmpty BSet = -1
	BSet0     BSet = 0
	BSet1     BSet = 1
	BSetStar  BSet = 2
)

const (
	StagePrevote     Stage = "prevote"
	StageMainvote    Stage = "mainvote"
	StageFinalvote   Stage = "finalvote"
	StageTermination Stage = "termination"
)
const (
	RBCTypeVal   RBCType = "val"
	RBCTypeEcho  RBCType = "echo"
	RBCTypeReady RBCType = "ready"
)

type FinalvoteRes struct {
	next_v  Val
	decided bool
}

// Set 是一个使用 map 实现的集合类型
type Set map[string]struct{}

// Add 向集合中添加一个元素
func (s Set) Add(item string) {
	s[item] = struct{}{}
}

// Remove 从集合中移除一个元素
func (s Set) Remove(item string) {
	delete(s, item)
}

// Contains 检查集合是否包含某个元素
func (s Set) Contains(item string) bool {
	_, exists := s[item]
	return exists
}

type StoreBase interface {
	prevote_insert_if_not_exists(
		session_id SessionId,
		node NodeId,
		r Round,
		sender NodeId,
		v Val,
	) bool
	prevote_get_n_support(
		session_id SessionId,
		node NodeId,
		r Round,
		v Val,
	) int
	get_bset(
		session_id SessionId,
		node NodeId,
		r Round,
	) BSet

	is_resolved(
		session_id SessionId,
		node NodeId,
		r Round,
		stage Stage,
	) bool

	set_resolved(
		session_id SessionId,
		node NodeId,
		r Round,
		stage Stage,
	)

	mainvote_insert_if_not_exists(
		session_id SessionId,
		node NodeId,
		r Round,
		sender NodeId,
		v Val,
	) bool
	mainvote_get(session_id SessionId, node NodeId, r Round, self_id NodeId) Val

	finalvote_get_proofs(session_id SessionId, node NodeId, r Round, origin Origin) map[NodeId]Set
	finalvote_insert_if_not_exists(
		session_id SessionId,
		node NodeId,
		r Round,
		origin Origin,
		sender Sender,
		witness NodeId,
		v Val,
	) bool
	termination_get_proofs(session_id SessionId, node NodeId) map[Origin]map[NodeId]Set
	termination_insert_if_not_exists(
		session_id SessionId,
		node NodeId,
		origin Origin,
		sender NodeId,
	) bool
	termination_get_stats(
		session_id SessionId,
		node NodeId,
	) int
}

type MemStore struct {
	self_id     NodeId
	store_mutex sync.RWMutex

	guard_r2_map map[SessionId]int
	guard_r1_map map[SessionId]int

	resolved         map[SessionId]map[NodeId]map[Round]map[Stage]bool
	prevote_resolved map[SessionId]map[NodeId]map[Round]map[Val]bool

	prevotes           map[SessionId]map[NodeId]map[Round]map[Val]Set
	mainvotes          map[SessionId]map[NodeId]map[Round]map[Val]Set
	finalvotes_proof   map[SessionId]map[NodeId]map[Round]map[Origin]map[Val]map[RBCType]Set
	terminations_proof map[SessionId]map[NodeId]map[Origin]map[RBCType]Set

	msg_queue_for_bset []Msg // 每一轮结束后清空
}

func (s *MemStore) termination_get_proofs(session_id SessionId, node NodeId) map[Origin]map[RBCType]Set {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	new_map := map[Origin]map[RBCType]Set{}
	/*
		if l1, ok := s.terminations[session_id]; ok {
			if l2, ok := l1[node]; ok {
				for origin, l3 := range l2 {
					new_map[origin] = map[Witness]Set{}
					for witness, _ := range l3 {
						new_map[origin][Witness(witness)] = Set{}
						for x, _ := range s.terminations_proof[session_id][node][origin][Witness(witness)] {
							new_map[origin][Witness(witness)].Add(x)
						}
					}
				}
			}
		}
	*/
	return new_map
}

func (s *MemStore) get_guard_r1(session_id SessionId) int {
	return s.guard_r1_map[session_id]
}
func (s *MemStore) get_guard_r2(session_id SessionId) int {
	return s.guard_r2_map[session_id]
}

func (s *MemStore) finalvote_get_proofs(session_id SessionId, node NodeId, r Round) map[Origin]map[Val]map[RBCType]Set {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	new_map := map[Origin]map[Val]map[RBCType]Set{}
	/*
		if l1, ok := s.finalvotes[session_id]; ok {
			if l2, ok := l1[node]; ok {
				if l3, ok := l2[r]; ok {
					for origin, l4 := range l3 {
						new_map[origin] = map[Val]map[Witness]Set{}
						for v, l5 := range l4 {
							new_map[origin][v] = map[Witness]Set{}
							for witness, _ := range l5 {
								new_map[origin][v][Witness(witness)] = Set{}
								for x, _ := range s.finalvotes_proof[session_id][node][r][origin][v][Witness(witness)] {
									new_map[origin][v][Witness(witness)].Add(x)
								}
							}
						}
					}
				}
			}
		}
	*/
	return new_map
}
func (s *MemStore) mainvote_get(session_id SessionId, node NodeId, r Round, self_id NodeId) Val {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	if l1, ok := s.mainvotes[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if l4, ok := l3[Val0]; ok {
					if l4.Contains(string(self_id)) {
						return Val0
					}
				}
				if l4, ok := l3[Val1]; ok {
					if l4.Contains(string(self_id)) {
						return Val1
					}
				}
			}
		}
	}
	return ValUnknown
}

func (s *MemStore) compute_mainvote_res(
	session_id SessionId,
	node NodeId,
	r Round,
) Val {
	v0 := 0
	v1 := 0
	guard_r2 := s.get_guard_r2(session_id)
	if l1, ok := s.mainvotes[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if l4, ok := l3[Val0]; ok {
					v0 = len(l4)
				}
				if l4, ok := l3[Val1]; ok {
					v1 = len(l4)
				}
			}
		}
	}
	if v0+v1 >= guard_r2 {

		if v0 >= guard_r2 {
			return Val0
		} else if v1 >= guard_r2 {
			return Val1
		} else {
			return ValStar
		}
	}
	return ValUnknown
}

func (s *MemStore) get_bset(
	session_id SessionId,
	node NodeId,
	r Round,
) BSet {
	flag0 := false
	flag1 := false
	guard_r2 := s.get_guard_r2(session_id)
	if l1, ok := s.prevotes[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if l4, ok := l3[Val0]; ok {
					if len(l4) >= guard_r2 {
						flag0 = true
					}
				}
				if l4, ok := l3[Val1]; ok {
					if len(l4) >= guard_r2 {
						flag1 = true
					}
				}
			}
		}
	}
	if flag0 && flag1 {
		return BSetStar
	}
	if flag0 {
		return BSet0
	}
	if flag1 {
		return BSet1
	}
	return BSetEmpty
}

func (s *MemStore) compute_termination_res(
	session_id SessionId,
	node NodeId,
) bool {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	n := 0
	guard_r2 := s.get_guard_r2(session_id)
	if l1, ok := s.terminations_proof[session_id]; ok {
		if l2, ok := l1[node]; ok {
			for _, origin := range l2 {
				if s, ok := origin[RBCTypeReady]; ok {
					if len(s) >= guard_r2 {
						n += 1
					}
				}
			}
		}
	}
	return n >= guard_r2
}
func (s *MemStore) compute_finalvote_res(
	session_id SessionId,
	node NodeId,
	r Round,
) (FinalvoteRes, bool) {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	t0 := 0
	t1 := 0
	t2 := 0
	guard_r2 := s.get_guard_r2(session_id)
	guard_r1 := s.get_guard_r1(session_id)
	if l1, ok := s.finalvotes_proof[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				for _, l4 := range l3 {
					if l5, ok := l4[Val0]; ok {
						if l6, ok := l5[RBCTypeReady]; ok {
							if len(l6) >= guard_r2 {
								t0 += 1
								continue
							}
						}
					}
					if l5, ok := l4[Val1]; ok {
						if l6, ok := l5[RBCTypeReady]; ok {
							if len(l6) >= guard_r2 {
								t1 += 1
								continue
							}
						}
					}
					if l5, ok := l4[ValStar]; ok {
						if l6, ok := l5[RBCTypeReady]; ok {
							if len(l6) >= guard_r2 {
								t2 += 1
								continue
							}
						}
					}
				}
			}
		}
	}

	if t0+t1+t2 >= guard_r2 {
		var next_v Val
		decided := false
		if t0 >= guard_r2 {
			decided = true
			next_v = Val0
		} else if t1 >= guard_r2 {
			decided = true
			next_v = Val1
		} else if t1 >= guard_r1 {
			next_v = Val1
		} else if t0 >= guard_r1 {
			next_v = Val0
		} else {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			randomInt := rng.Intn(2)
			if randomInt == 0 {
				next_v = Val0
			} else {
				next_v = Val1
			}
		}
		return FinalvoteRes{
			next_v:  next_v,
			decided: decided,
		}, true
	}
	return FinalvoteRes{}, false
}
func (s *MemStore) termination_insert_if_not_exists(
	session_id SessionId,
	node NodeId,
	origin Origin,
	rbc_type RBCType,
	prover Prover,
) bool {
	defer s.store_mutex.Unlock()
	s.store_mutex.Lock()
	exists := false
	if l1, ok := s.terminations_proof[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[origin]; ok {
				if l4, ok := l3[rbc_type]; ok {
					if l4.Contains(string(prover)) {
						exists = true
					}
				}
			}
		}
	}
	if exists {
		return false
	}
	if _, ok := s.terminations_proof[session_id]; !ok {
		s.terminations_proof[session_id] = make(map[NodeId]map[Origin]map[RBCType]Set)
	}
	if _, ok := s.terminations_proof[session_id][node]; !ok {
		s.terminations_proof[session_id][node] = make(map[Origin]map[RBCType]Set)
	}
	if _, ok := s.terminations_proof[session_id][node][origin]; !ok {
		s.terminations_proof[session_id][node][origin] = make(map[RBCType]Set)
	}
	if _, ok := s.terminations_proof[session_id][node][origin][rbc_type]; !ok {
		s.terminations_proof[session_id][node][origin][rbc_type] = Set{}
	}
	s.terminations_proof[session_id][node][origin][rbc_type].Add(string(prover))

	return true
}
func (s *MemStore) termination_get_stats(
	session_id SessionId,
	node NodeId,
	origin Origin,
) (int, int) {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()

	n_echo := 0
	n_ready := 0
	if l1, ok := s.terminations_proof[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[origin]; ok {
				if l6, ok := l3[RBCTypeEcho]; ok {
					n_echo = len(l6)
				}
				if l6, ok := l3[RBCTypeReady]; ok {
					n_ready = len(l6)
				}
			}
		}
	}
	return n_echo, n_ready

}
func (s *MemStore) finalvote_get_stats(
	session_id SessionId,
	node NodeId,
	r Round,
	origin Origin,
	v Val,
) (int, int) {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()

	n_echo := 0
	n_ready := 0
	if l1, ok := s.finalvotes_proof[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if l4, ok := l3[origin]; ok {
					if l5, ok := l4[v]; ok {
						if l6, ok := l5[RBCTypeEcho]; ok {
							n_echo = len(l6)
						}
						if l6, ok := l5[RBCTypeReady]; ok {
							n_ready = len(l6)
						}
					}
				}
			}
		}
	}
	return n_echo, n_ready

}
func (s *MemStore) finalvote_insert_if_not_exists(
	session_id SessionId,
	node NodeId,
	r Round,
	origin Origin,
	rbc_type RBCType,
	prover Prover,
	v Val,
) bool {
	defer s.store_mutex.Unlock()
	s.store_mutex.Lock()
	exists := false
	if l1, ok := s.finalvotes_proof[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if l4, ok := l3[origin]; ok {
					if l5, ok := l4[Val0]; ok {
						if l6, ok := l5[rbc_type]; ok {
							if l6.Contains(string(prover)) {
								exists = true
							}
						}
					}
					if l5, ok := l4[Val1]; ok {
						if l6, ok := l5[rbc_type]; ok {
							if l6.Contains(string(prover)) {
								exists = true
							}
						}
					}
					if l5, ok := l4[ValStar]; ok {
						if l6, ok := l5[rbc_type]; ok {
							if l6.Contains(string(prover)) {
								exists = true
							}
						}
					}
				}
			}
		}
	}
	if exists {
		return false
	}
	if _, ok := s.finalvotes_proof[session_id]; !ok {
		s.finalvotes_proof[session_id] = make(map[NodeId]map[Round]map[Origin]map[Val]map[RBCType]Set)
	}
	if _, ok := s.finalvotes_proof[session_id][node]; !ok {
		s.finalvotes_proof[session_id][node] = make(map[Round]map[Origin]map[Val]map[RBCType]Set)
	}
	if _, ok := s.finalvotes_proof[session_id][node][r]; !ok {
		s.finalvotes_proof[session_id][node][r] = make(map[Origin]map[Val]map[RBCType]Set)
	}
	if _, ok := s.finalvotes_proof[session_id][node][r][origin]; !ok {
		s.finalvotes_proof[session_id][node][r][origin] = make(map[Val]map[RBCType]Set)
	}
	if _, ok := s.finalvotes_proof[session_id][node][r][origin][v]; !ok {
		s.finalvotes_proof[session_id][node][r][origin][v] = make(map[RBCType]Set)
	}
	if _, ok := s.finalvotes_proof[session_id][node][r][origin][v][rbc_type]; !ok {
		s.finalvotes_proof[session_id][node][r][origin][v][rbc_type] = Set{}
	}
	s.finalvotes_proof[session_id][node][r][origin][v][rbc_type].Add(string(prover))

	return true
}
func (s *MemStore) prevote_insert_if_not_exists(
	session_id SessionId,
	node NodeId,
	r Round,
	sender NodeId,
	v Val,
) bool {
	defer s.store_mutex.Unlock()
	s.store_mutex.Lock()
	exists := false
	if l1, ok := s.prevotes[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if l4, ok := l3[v]; ok {
					if l4.Contains(string(sender)) {
						exists = true
					}
				}
			}
		}
	}
	if exists {
		return false
	}
	if _, ok := s.prevotes[session_id]; !ok {
		s.prevotes[session_id] = make(map[NodeId]map[Round]map[Val]Set)
	}
	if _, ok := s.prevotes[session_id][node]; !ok {
		s.prevotes[session_id][node] = make(map[Round]map[Val]Set)
	}
	if _, ok := s.prevotes[session_id][node][r]; !ok {
		s.prevotes[session_id][node][r] = make(map[Val]Set)
	}
	if _, ok := s.prevotes[session_id][node][r][v]; !ok {
		s.prevotes[session_id][node][r][v] = Set{}
	}
	s.prevotes[session_id][node][r][v].Add(string(sender))
	return true
}

func (s *MemStore) mainvote_insert_if_not_exists(
	session_id SessionId,
	node NodeId,
	r Round,
	sender NodeId,
	v Val,
) bool {
	defer s.store_mutex.Unlock()
	s.store_mutex.Lock()
	exists := false
	if l1, ok := s.mainvotes[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if l4, ok := l3[v]; ok {
					if l4.Contains(string(sender)) {
						exists = true
					}
				}
			}
		}
	}
	if exists {
		return false
	}
	if _, ok := s.mainvotes[session_id]; !ok {
		s.mainvotes[session_id] = make(map[NodeId]map[Round]map[Val]Set)
	}
	if _, ok := s.mainvotes[session_id][node]; !ok {
		s.mainvotes[session_id][node] = make(map[Round]map[Val]Set)
	}
	if _, ok := s.mainvotes[session_id][node][r]; !ok {
		s.mainvotes[session_id][node][r] = make(map[Val]Set)
	}
	if _, ok := s.mainvotes[session_id][node][r][v]; !ok {
		s.mainvotes[session_id][node][r][v] = Set{}
	}
	s.mainvotes[session_id][node][r][v].Add(string(sender))
	return true
}

func (s *MemStore) prevote_get_n_support(
	session_id SessionId,
	node NodeId,
	r Round,
	v Val,
) int {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	if l1, ok := s.prevotes[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if l4, ok := l3[v]; ok {
					return len(l4)
				}
			}
		}
	}
	return 0
}

func (s *MemStore) append_msg_queue_for_bset(msg Msg) {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	s.msg_queue_for_bset = append(s.msg_queue_for_bset, msg)
}

func (s *MemStore) get_msg_queue_for_bset() []Msg {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	res := []Msg{}
	for _, i := range s.msg_queue_for_bset {
		res = append(res, i)
	}
	s.msg_queue_for_bset = []Msg{}
	return res
}

func (s *MemStore) reset_msg_queue_for_bset() {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	s.msg_queue_for_bset = []Msg{}
}

func (s *MemStore) is_resolved(
	session_id SessionId,
	node NodeId,
	r Round,
	stage Stage,
) bool {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	if l1, ok := s.resolved[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if _, exists := l3[stage]; exists {
					return true
				}
			}
		}
	}
	return false
}

func (s *MemStore) prevote_is_resolved(
	session_id SessionId,
	node NodeId,
	r Round,
	v Val,
) bool {
	defer s.store_mutex.RUnlock()
	s.store_mutex.RLock()
	if l1, ok := s.prevote_resolved[session_id]; ok {
		if l2, ok := l1[node]; ok {
			if l3, ok := l2[r]; ok {
				if _, exists := l3[v]; exists {
					return true
				}
			}
		}
	}
	return false
}
func (s *MemStore) prevote_set_resolved(
	session_id SessionId,
	node NodeId,
	r Round,
	v Val,
) {
	defer s.store_mutex.Unlock()
	s.store_mutex.Lock()
	if _, ok := s.prevote_resolved[session_id]; !ok {
		s.prevote_resolved[session_id] = make(map[NodeId]map[Round]map[Val]bool)
	}
	if _, ok := s.prevote_resolved[session_id][node]; !ok {
		s.prevote_resolved[session_id][node] = make(map[Round]map[Val]bool)
	}
	if _, ok := s.prevote_resolved[session_id][node][r]; !ok {
		s.prevote_resolved[session_id][node][r] = make(map[Val]bool)
	}
	if _, ok := s.prevote_resolved[session_id][node][r][v]; !ok {
		s.prevote_resolved[session_id][node][r][v] = true
	}
}
func (s *MemStore) set_resolved(
	session_id SessionId,
	node NodeId,
	r Round,
	stage Stage,
) {
	defer s.store_mutex.Unlock()
	s.store_mutex.Lock()
	if _, ok := s.resolved[session_id]; !ok {
		s.resolved[session_id] = make(map[NodeId]map[Round]map[Stage]bool)
	}
	if _, ok := s.resolved[session_id][node]; !ok {
		s.resolved[session_id][node] = make(map[Round]map[Stage]bool)
	}
	if _, ok := s.resolved[session_id][node][r]; !ok {
		s.resolved[session_id][node][r] = make(map[Stage]bool)
	}
	if _, ok := s.resolved[session_id][node][r][stage]; !ok {
		s.resolved[session_id][node][r][stage] = true
	}
}

type ABAMsg struct {
	stage    Stage
	rbc_type RBCType
	node     NodeId
	origin   Origin
	r        Round
	v        Val
	no_reply bool
}
type Msg struct {
	category   string
	session_id SessionId
	sender     Sender
	target     Target
	data       ABAMsg
}
