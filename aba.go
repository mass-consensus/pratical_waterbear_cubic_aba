package pratical_waterbear_cubic_aba

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func aba(
	store *MemStore,
	guard_r1 int,
	guard_r2 int,
	session_id SessionId,
	self_id NodeId,
	v Val,
	node NodeId,
	aba_broadcast func(aba_msg ABAMsg),
	handle_outdated func(msg Msg),
	aba_ch chan Msg,
	aba_ctx context.Context,
	cancel context.CancelFunc,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	broadcast_prevote := func(r Round, v Val) {
		aba_broadcast(ABAMsg{
			node:  node,
			stage: StagePrevote,
			r:     r,
			v:     v,
		})
	}
	broadcast_mainvote := func(r Round, v Val) {
		aba_broadcast(ABAMsg{
			node:  node,
			stage: StageMainvote,
			r:     r,
			v:     v,
		})
	}
	broadcast_finalvote := func(r Round, origin Origin, rbc_type RBCType, v Val) {
		aba_broadcast(ABAMsg{
			node:     node,
			rbc_type: rbc_type,
			stage:    StageFinalvote,
			r:        r,
			v:        v,
			origin:   origin,
		})
	}
	broadcast_termination := func(origin Origin, rbc_type RBCType) {
		go func() {
			aba_broadcast(ABAMsg{
				node:     node,
				rbc_type: rbc_type,
				stage:    StageTermination,
				origin:   origin,
			})
		}()
	}

	prevote_broadcast_log := map[Round]map[Val]bool{}
	var prevote_broadcast_log_mutex sync.RWMutex
	handle_prevote := func(
		msg Msg,
		prevote_insert_if_not_exists func(sender Sender, v Val) bool,
		prevote_get_n_support func(v Val) int,
		is_resolved func(v Val) bool,
		set_resolved func(v Val),
		resolve func(v Val),
		handle_outdated func(msg Msg),
	) {
		defer prevote_broadcast_log_mutex.Unlock()
		prevote_broadcast_log_mutex.Lock()
		v := msg.data.v
		if !prevote_insert_if_not_exists(msg.sender, v) {
			if NodeId(msg.target) != NodeId(msg.sender) && !msg.data.no_reply {
				handle_outdated(msg)
			}
			return
		}
		total := prevote_get_n_support(v)
		if total >= guard_r1 {
			_, l1_exists := prevote_broadcast_log[msg.data.r]
			needs_broadcast := false
			if !l1_exists {
				prevote_broadcast_log[msg.data.r] = map[Val]bool{}
				needs_broadcast = true
			}
			_, l2_exists := prevote_broadcast_log[msg.data.r][v]
			if !l2_exists {
				prevote_broadcast_log[msg.data.r][v] = true
				needs_broadcast = true
			}
			if needs_broadcast {
				broadcast_prevote(msg.data.r, v)
			}
		}
		if total >= guard_r2 && !is_resolved(v) {
			set_resolved(v)
			resolve(v)
		}
	}

	handle_mainvote := func(
		msg Msg,
		mainvote_insert_if_not_exists func(sender Sender, v Val) bool,
		compute_mainvote_res func() Val,
		get_bset func() BSet,
		delay func(msg Msg),
		is_resolved func() bool,
		set_resolved func(),
		resolve func(v Val),
		handle_outdated func(msg Msg),
	) {
		v := msg.data.v
		bset := get_bset()
		if bset != BSetStar && v != Val(bset) {
			// 当bset变化时再处理
			delay(msg)
			return
		}
		if !mainvote_insert_if_not_exists(msg.sender, v) {
			if NodeId(msg.target) != NodeId(msg.sender) && !msg.data.no_reply {
				handle_outdated(msg)
			}
			return
		}
		res := compute_mainvote_res()
		if res != ValUnknown && !is_resolved() {
			set_resolved()
			resolve(res)
		}
	}

	finalvote_broadcast_log := map[Round]map[Origin]map[Val]map[RBCType]bool{}
	var finalvote_broadcast_log_mutex sync.RWMutex
	finalvote_broadcast_log_insert_if_not_exists := func(r Round, origin Origin, v Val, rbc_type RBCType) bool {
		defer finalvote_broadcast_log_mutex.Unlock()
		finalvote_broadcast_log_mutex.Lock()

		if _, ok := finalvote_broadcast_log[r]; !ok {
			finalvote_broadcast_log[r] = map[Origin]map[Val]map[RBCType]bool{}
		}
		if _, ok := finalvote_broadcast_log[r][origin]; !ok {
			finalvote_broadcast_log[r][origin] = map[Val]map[RBCType]bool{}
		}
		if _, ok := finalvote_broadcast_log[r][origin][v]; !ok {
			finalvote_broadcast_log[r][origin][v] = map[RBCType]bool{}
		}
		if _, ok := finalvote_broadcast_log[r][origin][v][rbc_type]; !ok {
			finalvote_broadcast_log[r][origin][v][rbc_type] = true
			return true
		}
		return false
	}
	handle_finalvote := func(
		msg Msg,
		get_stats func(r Round, origin Origin, v Val) (int, int),
		finalvote_insert_if_not_exists func(origin Origin, rbc_type RBCType, prover Prover, v Val) bool,
		compute_finalvote_res func() (FinalvoteRes, bool),
		get_bset func() BSet,
		delay func(msg Msg),
		is_resolved func() bool,
		set_resolved func(),
		resolve func(res FinalvoteRes),
		handle_outdated func(msg Msg),
	) {
		v := msg.data.v
		bset := get_bset()
		if bset != BSetStar && BSet(v) != bset {
			// 当bset变化时再处理
			delay(msg)
			return
		}
		if !finalvote_insert_if_not_exists(msg.data.origin, msg.data.rbc_type, Prover(msg.sender), v) {
			if NodeId(msg.target) != NodeId(msg.sender) && !msg.data.no_reply {
				handle_outdated(msg)
			}
			return
		}

		n_echo, n_ready := get_stats(msg.data.r, msg.data.origin, msg.data.v)
		// 当收到RBC val, 广播RBC echo
		if msg.data.rbc_type == RBCTypeVal {
			if finalvote_broadcast_log_insert_if_not_exists(msg.data.r, msg.data.origin, msg.data.v, RBCTypeEcho) {
				broadcast_finalvote(msg.data.r, msg.data.origin, RBCTypeEcho, v)
			}
		}
		// 当收到r1个RBC ready或r2个echo, 广播ready
		if n_echo >= guard_r2 || n_ready >= guard_r1 {
			if finalvote_broadcast_log_insert_if_not_exists(msg.data.r, msg.data.origin, msg.data.v, RBCTypeReady) {
				broadcast_finalvote(msg.data.r, msg.data.origin, RBCTypeReady, v)
			}
		}

		if is_resolved() {
			return
		}
		if res, ok := compute_finalvote_res(); ok {
			set_resolved()
			resolve(res)
		}
	}

	termination_broadcast_log := map[Origin]map[RBCType]bool{}
	var termination_broadcast_log_mutex sync.RWMutex
	termination_broadcast_log_insert_if_not_exists := func(origin Origin, rbc_type RBCType) bool {
		defer termination_broadcast_log_mutex.Unlock()
		termination_broadcast_log_mutex.Lock()

		if _, ok := termination_broadcast_log[origin]; !ok {
			termination_broadcast_log[origin] = map[RBCType]bool{}
		}
		if _, ok := termination_broadcast_log[origin][rbc_type]; !ok {
			termination_broadcast_log[origin][rbc_type] = true
			return true
		}
		return false
	}
	handle_termination := func(
		msg Msg,
		get_stats func(origin Origin) (int, int),
		termination_insert_if_not_exists func(origin Origin, rbc_type RBCType, prover Prover) bool,
		compute_termination_res func() bool,
		is_resolved func() bool,
		set_resolved func(),
		resolve func(),
		handle_outdated func(msg Msg),
	) {
		if !termination_insert_if_not_exists(msg.data.origin, msg.data.rbc_type, Prover(msg.sender)) {
			if NodeId(msg.target) != NodeId(msg.sender) && !msg.data.no_reply {
				handle_outdated(msg)
			}
			return
		}

		n_echo, n_ready := get_stats(msg.data.origin)

		// 当收到RBC val 广播RBC echo
		if msg.data.rbc_type == RBCTypeVal {
			if termination_broadcast_log_insert_if_not_exists(msg.data.origin, RBCTypeEcho) {
				broadcast_termination(msg.data.origin, RBCTypeEcho)
			}
		}
		// 当收到r1个RBC ready或r2个echo, 广播ready
		if n_echo >= guard_r2 || n_ready >= guard_r1 {
			if termination_broadcast_log_insert_if_not_exists(msg.data.origin, RBCTypeReady) {
				broadcast_termination(msg.data.origin, RBCTypeReady)
			}
		}

		if is_resolved() {
			return
		}

		if compute_termination_res() {
			set_resolved()
			resolve()
		}
	}

	var r_ctx_map sync.Map
	var r_ctx_cancel_map sync.Map
	var prevote_ctx_map sync.Map
	var prevote_ctx_cancel_map sync.Map
	var mainvote_ctx_map sync.Map
	var mainvote_ctx_cancel_map sync.Map
	var finalvote_ctx_map sync.Map
	var finalvote_ctx_cancel_map sync.Map
	reset_ctx_map := func() {
		r_ctx_map.Range(func(key, value interface{}) bool {
			r_ctx_map.Delete(key)
			if cancel, ok := r_ctx_cancel_map.LoadAndDelete(key); ok {
				cancel.(context.CancelFunc)()
			}
			return true
		})
		prevote_ctx_map.Range(func(key, value interface{}) bool {
			prevote_ctx_map.Delete(key)
			if cancel, ok := prevote_ctx_cancel_map.LoadAndDelete(key); ok {
				cancel.(context.CancelFunc)()
			}
			return true
		})
		mainvote_ctx_map.Range(func(key, value interface{}) bool {
			mainvote_ctx_map.Delete(key)
			if cancel, ok := mainvote_ctx_cancel_map.LoadAndDelete(key); ok {
				cancel.(context.CancelFunc)()
			}
			return true
		})
		finalvote_ctx_map.Range(func(key, value interface{}) bool {
			finalvote_ctx_map.Delete(key)
			if cancel, ok := finalvote_ctx_cancel_map.LoadAndDelete(key); ok {
				cancel.(context.CancelFunc)()
			}
			return true
		})
	}

	r_msg_handler := func(r Round, msg Msg, prevote_resolve func(res Val), mainvote_resolve func(res Val), finalvote_resolve func(res FinalvoteRes), termination_resolve func()) {
		if msg.data.stage == StagePrevote {
			handle_prevote(
				msg,
				func(sender Sender, v Val) bool {
					return store.prevote_insert_if_not_exists(session_id, node, r, NodeId(sender), v)
				}, func(v Val) int {
					return store.prevote_get_n_support(session_id, node, r, v)
				}, func(v Val) bool {
					return store.prevote_is_resolved(session_id, node, r, v)
				}, func(v Val) {
					store.prevote_set_resolved(session_id, node, r, v)
				}, func(v Val) {
					prevote_resolve(v)
				},
				handle_outdated,
			)
		} else if msg.data.stage == StageMainvote {
			handle_mainvote(msg,
				func(sender Sender, v Val) bool {
					return store.mainvote_insert_if_not_exists(session_id, node, r, NodeId(sender), v)
				}, func() Val {
					return store.compute_mainvote_res(session_id, node, r)
				}, func() BSet {
					return store.get_bset(session_id, node, r)
				}, func(msg Msg) {
					store.append_msg_queue_for_bset(msg)
				}, func() bool {
					return store.is_resolved(session_id, node, r, StageMainvote)
				}, func() {
					store.set_resolved(session_id, node, r, StageMainvote)
				}, mainvote_resolve,
				handle_outdated,
			)
		} else if msg.data.stage == StageFinalvote {
			handle_finalvote(msg,
				func(r Round, origin Origin, v Val) (int, int) {
					return store.finalvote_get_stats(session_id, node, r, origin, v)
				},
				func(origin Origin, rbc_type RBCType, prover Prover, v Val) bool {
					return store.finalvote_insert_if_not_exists(session_id, node, r, origin, rbc_type, prover, v)
				},
				func() (FinalvoteRes, bool) {
					return store.compute_finalvote_res(session_id, node, r)
				}, func() BSet {
					return store.get_bset(session_id, node, r)
				}, func(msg Msg) {
					store.append_msg_queue_for_bset(msg)
				}, func() bool {
					return store.is_resolved(session_id, node, r, StageFinalvote)
				}, func() {
					store.set_resolved(session_id, node, r, StageFinalvote)
				}, finalvote_resolve,
				handle_outdated,
			)
		} else if msg.data.stage == StageTermination {
			handle_termination(msg,
				func(origin Origin) (int, int) {
					return store.termination_get_stats(session_id, node, origin)
				},
				func(origin Origin, rbc_type RBCType, prover Prover) bool {
					return store.termination_insert_if_not_exists(session_id, node, origin, rbc_type, prover)
				},
				func() bool {
					return store.compute_termination_res(session_id, node)
				},
				func() bool {
					return store.is_resolved(session_id, node, 0, StageTermination)
				},
				func() {
					store.set_resolved(session_id, node, 0, StageTermination)
				},
				termination_resolve,
				handle_outdated,
			)
		}
	}

	process_round := func(r Round, v Val) {
		if r_ctx_cancel, ok := r_ctx_cancel_map.Load(r); ok {
			defer r_ctx_cancel.(context.CancelFunc)()
		}
		fmt.Println("id", self_id, "round", r, "v", v)
		prevote_ctx, prevote_ctx_cancel := context.WithCancel(context.Background())
		prevote_ctx_map.Store(r, prevote_ctx)
		prevote_ctx_cancel_map.Store(r, prevote_ctx_cancel)
		mainvote_ctx, mainvote_ctx_cancel := context.WithCancel(context.Background())
		mainvote_ctx_map.Store(r, mainvote_ctx)
		mainvote_ctx_cancel_map.Store(r, mainvote_ctx_cancel)
		finalvote_ctx, finalvote_ctx_cancel := context.WithCancel(context.Background())
		finalvote_ctx_map.Store(r, finalvote_ctx)
		finalvote_ctx_cancel_map.Store(r, finalvote_ctx_cancel)

		var first_prevote_res Val
		if store.prevote_is_resolved(session_id, node, r, v) {
			first_prevote_res = v
		} else {
			exit := false
			for !exit {
				broadcast_prevote(r, v)
				prevote_ctx, exists := prevote_ctx_map.Load(r)
				if !exists {
					return
				}
				select {
				case <-prevote_ctx.(context.Context).Done():
					if new_prevote_ctx, exists := prevote_ctx_map.Load(r); exists {
						if res, ok := new_prevote_ctx.(context.Context).Value("res").(Val); ok {
							first_prevote_res = res
							exit = true
							continue
						}
					}
					return
				case <-aba_ctx.Done():
					return
				case <-time.After(1000 * time.Millisecond):
					fmt.Println("prevote超时重试", self_id, r)
					continue
				}
			}
		}

		fmt.Println("id", self_id, "round", r, "prevote_res", first_prevote_res)

		var mainvote_res Val
		if store.is_resolved(session_id, node, r, StageMainvote) {
			mainvote_res = store.compute_mainvote_res(session_id, node, r)
		} else {
			exit := false
			for !exit {
				broadcast_mainvote(r, first_prevote_res)
				mainvote_ctx, exists := mainvote_ctx_map.Load(r)
				if !exists {
					return
				}
				select {
				case <-mainvote_ctx.(context.Context).Done():
					if new_mainvote_ctx, exists := mainvote_ctx_map.Load(r); exists {
						if res, ok := new_mainvote_ctx.(context.Context).Value("res").(Val); ok {
							mainvote_res = res
							exit = true
							continue
						}
					}
					return
				case <-aba_ctx.Done():
					return
				case <-time.After(1000 * time.Millisecond):
					fmt.Println("mainvote超时重试", self_id, r)
					continue
				}
			}
		}
		fmt.Println("id", self_id, "round", r, "mainvote_res", mainvote_res)

		var finalvote_res FinalvoteRes
		if store.is_resolved(session_id, node, r, StageFinalvote) {
			if res, ok := store.compute_finalvote_res(session_id, node, r); ok {
				finalvote_res = res
			}
		} else {
			exit := false
			for !exit {
				broadcast_finalvote(r, Origin(self_id), RBCTypeVal, mainvote_res)
				finalvote_ctx, exists := finalvote_ctx_map.Load(r)
				if !exists {
					return
				}
				select {

				case <-finalvote_ctx.(context.Context).Done():
					if new_finalvote_ctx, exists := finalvote_ctx_map.Load(r); exists {
						if res, ok := new_finalvote_ctx.(context.Context).Value("res").(FinalvoteRes); ok {
							finalvote_res = res
							exit = true
							continue
						}
					}
					return
				case <-aba_ctx.Done():
					return
				case <-time.After(1000 * time.Millisecond):
					fmt.Println("finalvote超时重试", self_id, r)
					/*
					   for each origin v
					       如果已经收到r1个ready或者r2个echo 广播ready
					       如果已经收到val 广播echo
					*/
					proofs := store.finalvote_get_proofs(session_id, node, r)
					for origin, proofs1 := range proofs {
						for v, x := range proofs1 {
							n_echo, n_ready := store.finalvote_get_stats(session_id, node, r, origin, v)
							if n_echo >= guard_r2 || n_ready >= guard_r1 {
								broadcast_finalvote(r, origin, RBCTypeReady, v)
							}

							fmt.Println(n_echo, n_ready)
							if t, ok := x[RBCTypeVal]; ok {
								if t.Contains(string(origin)) {
									broadcast_finalvote(r, origin, RBCTypeEcho, v)
								}
							}
						}
					}
				}
			}
		}
		fmt.Println("id", self_id, "round", r, "finalvote_res", finalvote_res)

		if _, loaded := prevote_ctx_map.LoadAndDelete(r - 1); loaded {
			if ctx_cancel, loaded := prevote_ctx_cancel_map.LoadAndDelete(r - 1); loaded {
				ctx_cancel.(context.CancelFunc)()
			}
		}
		if _, loaded := mainvote_ctx_map.LoadAndDelete(r - 1); loaded {
			if ctx_cancel, loaded := mainvote_ctx_cancel_map.LoadAndDelete(r - 1); loaded {
				ctx_cancel.(context.CancelFunc)()
			}
		}
		if _, loaded := finalvote_ctx_map.LoadAndDelete(r - 1); loaded {
			if ctx_cancel, loaded := finalvote_ctx_cancel_map.LoadAndDelete(r - 1); loaded {
				ctx_cancel.(context.CancelFunc)()
			}
		}

		if r_ctx, ok := r_ctx_map.Load(r); ok {
			r_ctx_map.Store(r, context.WithValue(r_ctx.(context.Context), "res", finalvote_res))
		}
	}

	var r Round = 0
	var r_mutex sync.RWMutex
	go func(aba_ctx context.Context) {
		for {
			select {
			case <-aba_ctx.Done():
				return
			case msg, ok := <-aba_ch:
				if !ok {
					return
				}
				if msg.data.r > r {
					// TODO 使用map存储
					// 缓存最近1轮的消息(延时重放)，过于超前的丢弃
					// fmt.Println("== 超前消息", self_id, msg)
					if msg.data.r == r+1 {
						go (func() {
							time.Sleep(1000 * time.Millisecond)
							aba_ch <- msg
						})()
					}
					continue
				}
				if msg.data.stage != StageTermination && msg.data.r < r-1 {
					// fmt.Println("过期消息", msg)
					handle_outdated(msg)
				} else {
					r_msg_handler(msg.data.r, msg, func(res Val) {
						if ctx, exists := prevote_ctx_map.Load(msg.data.r); exists {
							prevote_ctx_map.Store(msg.data.r, context.WithValue(ctx.(context.Context), "res", res))
							if cancel, exists := prevote_ctx_cancel_map.Load(msg.data.r); exists {
								cancel.(context.CancelFunc)()
							}
						}
						for _, m := range store.get_msg_queue_for_bset() {
							aba_ch <- m
						}
					}, func(res Val) {
						if ctx, exists := mainvote_ctx_map.Load(msg.data.r); exists {
							mainvote_ctx_map.Store(msg.data.r, context.WithValue(ctx.(context.Context), "res", res))
							if cancel, exists := mainvote_ctx_cancel_map.Load(msg.data.r); exists {
								cancel.(context.CancelFunc)()
							}
						}
					}, func(res FinalvoteRes) {
						if ctx, exists := finalvote_ctx_map.Load(msg.data.r); exists {
							finalvote_ctx_map.Store(msg.data.r, context.WithValue(ctx.(context.Context), "res", res))
							if cancel, exists := finalvote_ctx_cancel_map.Load(msg.data.r); exists {
								cancel.(context.CancelFunc)()
							}
						}
					}, func() {
						cancel()
					})
				}
			}
		}
	}(aba_ctx)

	next_v := v
	go func(aba_ctx context.Context) {
		for {
			store.reset_msg_queue_for_bset()
			r_ctx, cancel := context.WithCancel(context.Background())
			r_ctx_map.Store(r, r_ctx)
			r_ctx_cancel_map.Store(r, cancel)
			process_round(r, next_v)
			select {
			case <-aba_ctx.Done():
				{
					return
				}
			case <-r_ctx.Done():
				if r_ctx, ok := r_ctx_map.Load(r); ok {
					if finalvote_res, ok := r_ctx.(context.Context).Value("res").(FinalvoteRes); ok {
						next_v = finalvote_res.next_v
						if finalvote_res.decided {
							/*
									每轮结束都发Val广播

								    for each origin v
								        如果已经收到r1个ready或者r2个echo 广播ready
								        如果已经收到val 广播echo
							*/
							broadcast_termination(Origin(self_id), RBCTypeVal)
							proofs := store.termination_get_proofs(session_id, node)
							for origin, proofs1 := range proofs {
								n_echo, n_ready := store.termination_get_stats(session_id, node, origin)
								if n_echo >= guard_r2 || n_ready >= guard_r1 {
									broadcast_termination(origin, RBCTypeReady)
								}

								if t, ok := proofs1[RBCTypeVal]; ok {
									if t.Contains(string(origin)) {
										broadcast_termination(origin, RBCTypeEcho)
									}
								}
							}
						}
						r_mutex.Lock()
						r++
						r_mutex.Unlock()
						continue
					}
				}
				return
			}
		}
	}(aba_ctx)

	<-aba_ctx.Done()
	reset_ctx_map()
	fmt.Println(self_id, "exit", next_v)
}
