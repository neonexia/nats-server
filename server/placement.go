// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// PlacementConfig enables process-scoped placement admission. The HTTP control
// listener belongs on a private management network, never the public proxy.
type PlacementConfig struct {
	ControlUser   string `json:"controlUser"`
	ControlListen string `json:"controlListen"`
	ControlToken  string `json:"-"`
}

// PlacementBinding identifies one authorized activation of this process.
type PlacementBinding struct {
	BootID      string `json:"bootId"`
	ShardID     string `json:"shardId"`
	Incarnation string `json:"incarnation"`
	OperationID string `json:"operationId"`
}

// PlacementStatus contains no credentials or payload data.
type PlacementStatus struct {
	BootID  string            `json:"bootId"`
	Binding *PlacementBinding `json:"binding,omitempty"`
	Active  bool              `json:"active"`
	Fenced  bool              `json:"fenced"`
}

type placementState struct {
	mu       sync.Mutex
	bootID   string
	binding  *PlacementBinding
	active   bool
	fenced   bool
	http     *http.Server
	listener net.Listener
}

func parsePlacementConfig(v any) (*PlacementConfig, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("placement must be a configuration block")
	}
	p := &PlacementConfig{}
	for k, v := range m {
		_, v = unwrapValue(v, nil)
		value, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("placement %s must be a string", k)
		}
		switch strings.ToLower(k) {
		case "control_user":
			p.ControlUser = value
		case "control_listen":
			p.ControlListen = value
		case "control_token":
			p.ControlToken = value
		default:
			return nil, fmt.Errorf("unknown placement option %q", k)
		}
	}
	return p, nil
}

func validatePlacementConfig(o *Options) error {
	p := o.Placement
	if p == nil {
		return nil
	}
	if p.ControlUser != "" && (o.NoAuthUser == p.ControlUser || o.Websocket.NoAuthUser == p.ControlUser) {
		return errors.New("placement control_user cannot be a no_auth_user")
	}
	if p.ControlUser == "" || len(p.ControlToken) < 16 {
		return errors.New("placement requires control_user and control_token of at least 16 bytes")
	}
	if _, _, err := net.SplitHostPort(p.ControlListen); err != nil {
		return fmt.Errorf("placement control_listen: %w", err)
	}
	// These ingress paths do not run the application CONNECT placement gate.
	// Reject them rather than silently permitting a bypass on an opted-in slot.
	if o.Cluster.Port != 0 || len(o.Routes) != 0 || o.Gateway.Port != 0 || len(o.Gateway.Gateways) != 0 || o.LeafNode.Port != 0 || len(o.LeafNode.Remotes) != 0 || o.MQTT.Port != 0 {
		return errors.New("placement admission requires a standalone slot without routes, gateways, leaf nodes or MQTT")
	}
	// Until other identity mechanisms expose a verified principal, the exemption is
	// deliberately limited to an explicitly configured password user.
	for _, u := range o.Users {
		if u.Username == p.ControlUser && u.Password != "" {
			return nil
		}
	}
	return errors.New("placement control_user must name a configured password user")
}

func placementIdentifier(v string) bool {
	if len(v) == 0 || len(v) > 128 {
		return false
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// PlacementStatus returns a copy; the binding is never a live mutable pointer.
func (s *Server) PlacementStatus() PlacementStatus {
	p := s.placement
	if p == nil {
		return PlacementStatus{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statusLocked()
}

func (p *placementState) statusLocked() PlacementStatus {
	status := PlacementStatus{BootID: p.bootID, Active: p.active, Fenced: p.fenced}
	if p.binding != nil {
		binding := *p.binding
		status.Binding = &binding
	}
	return status
}

func (s *Server) placementControlClient(c *client) bool {
	opts := s.getOpts()
	if opts.Placement == nil {
		return false
	}
	c.mu.Lock()
	user, password := c.opts.Username, c.opts.Password
	c.mu.Unlock()
	if user != opts.Placement.ControlUser {
		return false
	}
	// Re-check the actual configured credential so a JWT/name assertion or custom
	// authenticator cannot manufacture the privileged placement exemption.
	for _, u := range opts.Users {
		if u.Username == user && u.Password != "" {
			return comparePasswords(u.Password, password)
		}
	}
	return false
}

func (s *Server) placementAdmits(c *client) bool {
	p := s.placement
	if p == nil || s.placementControlClient(c) {
		return true
	}
	c.mu.Lock()
	name := c.opts.Name
	c.mu.Unlock()
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "alini" || parts[1] != "1" || !placementIdentifier(parts[2]) || !placementIdentifier(parts[3]) {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active && !p.fenced && p.binding != nil && parts[2] == p.binding.ShardID && parts[3] == p.binding.Incarnation
}

func (s *Server) changePlacement(action string, binding PlacementBinding) (PlacementStatus, error) {
	p := s.placement
	p.mu.Lock()
	defer p.mu.Unlock()
	if binding.BootID != p.bootID {
		return p.statusLocked(), errors.New("stale process boot identity")
	}
	if p.binding != nil && *p.binding != binding {
		return p.statusLocked(), errors.New("placement binding conflicts with this process")
	}
	switch action {
	case "bind":
		if p.fenced {
			return p.statusLocked(), errors.New("placement is fenced; restart required")
		}
		if p.binding == nil {
			copy := binding
			p.binding = &copy
		}
	case "activate":
		if p.fenced {
			return p.statusLocked(), errors.New("placement is fenced; restart required")
		}
		if p.binding == nil {
			return p.statusLocked(), errors.New("placement is not bound")
		}
		p.active = true
	case "fence", "reset":
		if p.binding == nil && action != "reset" {
			return p.statusLocked(), errors.New("placement is not bound")
		}
		p.active = false
		p.fenced = true
	default:
		return p.statusLocked(), errors.New("unknown placement operation")
	}
	return p.statusLocked(), nil
}

func (s *Server) servePlacementControl(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if r.Header.Get("Authorization") == token || subtle.ConstantTimeCompare([]byte(token), []byte(s.getOpts().Placement.ControlToken)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/v1/placement" && r.Method == http.MethodGet {
		json.NewEncoder(w).Encode(s.PlacementStatus())
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/v1/placement/")
	if action != "bind" && action != "activate" && action != "fence" && action != "reset" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var binding PlacementBinding
	if err := decoder.Decode(&binding); err != nil {
		http.Error(w, "invalid placement request", 400)
		return
	}
	unboundReset := action == "reset" && binding.ShardID == "" && binding.Incarnation == "" && binding.OperationID == ""
	if err := decoder.Decode(&struct{}{}); err != io.EOF || !placementIdentifier(binding.BootID) || (!unboundReset && (!placementIdentifier(binding.ShardID) || !placementIdentifier(binding.Incarnation) || !placementIdentifier(binding.OperationID))) {
		http.Error(w, "invalid placement identity", 400)
		return
	}
	if action == "reset" && s.placementHasStreams() {
		http.Error(w, "placement reset requires an empty JetStream store; retire or archive streams first", http.StatusConflict)
		return
	}
	status, err := s.changePlacement(action, binding)
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if action == "fence" || action == "reset" {
		// Admission is disabled before collecting clients. Clients created concurrently
		// fail admission; existing clients, including pipelined handshakes, are closed.
		s.mu.RLock()
		clients := make([]*client, 0, len(s.clients))
		for _, c := range s.clients {
			clients = append(clients, c)
		}
		s.mu.RUnlock()
		for _, c := range clients {
			if !s.placementControlClient(c) {
				c.sendErr("Placement unavailable")
				c.closeConnection(AuthenticationViolation)
			}
		}
	}
	s.Noticef("Placement %s: shard=%s operation=%s", action, binding.ShardID, binding.OperationID)
	if action == "reset" {
		w.WriteHeader(http.StatusAccepted)
	}
	json.NewEncoder(w).Encode(status)
	if action == "reset" {
		// Flush the acknowledgement before stopping the management listener. A lost
		// response is recovered by observing a different boot ID after restart.
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go s.Shutdown()
	}
}

// Reset must not turn an aborted restore's retained files into an apparently
// empty reusable slot. The lifecycle owner must complete durable cleanup first.
func (s *Server) placementHasStreams() bool {
	hasStreams := false
	s.accounts.Range(func(_, value any) bool {
		if value.(*Account).numStreams() > 0 {
			hasStreams = true
			return false
		}
		return true
	})
	return hasStreams
}

// PlacementControlAddr is the private listener address, useful when port zero is
// selected by a development harness. It is never advertised in client INFO.
func (s *Server) PlacementControlAddr() net.Addr {
	if s.placement == nil {
		return nil
	}
	s.placement.mu.Lock()
	defer s.placement.mu.Unlock()
	if s.placement.listener == nil {
		return nil
	}
	return s.placement.listener.Addr()
}

func (s *Server) startPlacementControl() error {
	p := s.placement
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.shutdown.Load() {
		return errors.New("server is shutting down")
	}
	listener, err := net.Listen("tcp", s.getOpts().Placement.ControlListen)
	if err != nil {
		return err
	}
	p.listener = listener
	p.http = &http.Server{Handler: http.HandlerFunc(s.servePlacementControl), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	go func() {
		if err := p.http.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.Errorf("Placement control: %v", err)
		}
	}()
	return nil
}
func (s *Server) stopPlacementControl() {
	p := s.placement
	if p == nil {
		return
	}
	p.mu.Lock()
	server := p.http
	p.mu.Unlock()
	if server != nil {
		server.Close()
	}
}
