package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	Version     = "KV/1.0"
	DefaultAddr = "127.0.0.1:5050"
)

type store struct {
	mu   sync.RWMutex
	data map[string]string
}

func newStore() *store {
	return &store{data: make(map[string]string)}
}

func (s *store) put(k, v string) (created bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.data[k]
	s.data[k] = v
	return !existed
}

func (s *store) get(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[k]
	return v, ok
}

func (s *store) del(k string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[k]; ok {
		delete(s.data, k)
		return true
	}
	return false
}

func (s *store) size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

type stats struct {
	StartTime   time.Time
	TotalConns  int64
	ActiveConns int64
	ReqCount    int64
	PutCount    int64
	GetCount    int64
	DelCount    int64
}

type server struct {
	addr    string
	store   *store
	statsMu sync.Mutex
	stats   stats
}

func newServer(addr string) *server {
	return &server{
		addr:  addr,
		store: newStore(),
		stats: stats{StartTime: time.Now()},
	}
}

func (sv *server) incr(field *int64, delta int64) {
	sv.statsMu.Lock()
	*field += delta
	sv.statsMu.Unlock()
}

func (sv *server) snapshotStats() map[string]any {
	sv.statsMu.Lock()
	defer sv.statsMu.Unlock()
	uptime := time.Since(sv.stats.StartTime).Seconds()
	return map[string]any{
		"version":      Version,
		"uptime_sec":   int(uptime),
		"total_conns":  sv.stats.TotalConns,
		"active_conns": sv.stats.ActiveConns,
		"req_count":    sv.stats.ReqCount,
		"put_count":    sv.stats.PutCount,
		"get_count":    sv.stats.GetCount,
		"del_count":    sv.stats.DelCount,
		"keys":         sv.store.size(),
	}
}

func (sv *server) handleConn(c net.Conn) {
	sv.incr(&sv.stats.TotalConns, 1)
	sv.incr(&sv.stats.ActiveConns, 1)
	defer func() {
		sv.incr(&sv.stats.ActiveConns, -1)
		_ = c.Close()
	}()

	r := bufio.NewReader(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			// client đóng kết nối
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			// bỏ qua dòng rỗng
			continue
		}

		sv.incr(&sv.stats.ReqCount, 1)

		// Parse: KV/1.0 <CMD> [args...]
		toks := strings.Fields(line)
		if len(toks) < 2 {
			sv.writeResp(c, "400 BAD_REQUEST\n")
			continue
		}
		if toks[0] != Version {
			sv.writeResp(c, "426 UPGRADE_REQUIRED\n")
			continue
		}

		cmd := strings.ToUpper(toks[1])
		switch cmd {
		case "PUT":
			if len(toks) < 4 {
				sv.writeResp(c, "400 BAD_REQUEST\n")
				continue
			}
			key := toks[2]
			value := toks[3]
			created := sv.store.put(key, value)
			sv.incr(&sv.stats.PutCount, 1)
			if created {
				sv.writeResp(c, "201 CREATED\n")
			} else {
				sv.writeResp(c, "200 OK\n")
			}

		case "GET":
			if len(toks) != 3 {
				sv.writeResp(c, "400 BAD_REQUEST\n")
				continue
			}
			key := toks[2]
			if val, ok := sv.store.get(key); ok {
				sv.incr(&sv.stats.GetCount, 1)
				sv.writeResp(c, fmt.Sprintf("200 OK %s\n", val))
			} else {
				sv.writeResp(c, "404 NOT_FOUND\n")
			}

		case "DEL":
			if len(toks) != 3 {
				sv.writeResp(c, "400 BAD_REQUEST\n")
				continue
			}
			key := toks[2]
			if sv.store.del(key) {
				sv.incr(&sv.stats.DelCount, 1)
				sv.writeResp(c, "204 NO_CONTENT\n")
			} else {
				sv.writeResp(c, "404 NOT_FOUND\n")
			}

		case "STATS":
			if len(toks) != 2 {
				sv.writeResp(c, "400 BAD_REQUEST\n")
				continue
			}
			payload, _ := json.Marshal(sv.snapshotStats())
			// data trả ra dạng JSON theo sau 200 OK
			sv.writeResp(c, fmt.Sprintf("200 OK %s\n", string(payload)))

		case "QUIT":
			sv.writeResp(c, "200 OK bye\n")
			return

		default:
			sv.writeResp(c, "400 BAD_REQUEST\n")
		}
	}
}

func (sv *server) writeResp(c net.Conn, s string) {
	_, _ = c.Write([]byte(s))
}

func (sv *server) run() error {
	ln, err := net.Listen("tcp", sv.addr)
	if err != nil {
		return err
	}
	fmt.Printf("[KVSS] listening on %s\n", sv.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		go sv.handleConn(conn)
	}
}

func main() {
	addr := DefaultAddr
	if len(os.Args) > 1 && os.Args[1] != "" {
		addr = os.Args[1]
	}
	sv := newServer(addr)
	if err := sv.run(); err != nil {
		fmt.Println("SERVER_ERROR:", err)
		os.Exit(1)
	}
}
