// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const s3IndexPageSize = 64

// Pages are content addressed and bounded at every level. Replacing the path
// to one leaf never rewrites unrelated history or an unbounded root directory.
type s3IndexRef struct {
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
	First  uint32 `json:"first"`
	Last   uint32 `json:"last"`
	Count  uint64 `json:"count"`
	Level  uint8  `json:"level"`
}

type s3IndexPage struct {
	Version     uint8               `json:"version"`
	Incarnation string              `json:"incarnation"`
	Level       uint8               `json:"level"`
	Blocks      []s3BlockDescriptor `json:"blocks,omitempty"`
	Children    []s3IndexRef        `json:"children,omitempty"`
}

type s3IndexPending struct {
	Manifest s3TierManifest      `json:"manifest"`
	Added    []s3BlockDescriptor `json:"added,omitempty"`
	Obsolete []s3IndexRef        `json:"obsolete,omitempty"`
}

func (t *fileS3Tier) indexPageKey(hash string) string {
	return fmt.Sprintf("%s/index/pages/%s.json", strings.Trim(t.cfg.Prefix, "/"), hash)
}

func (t *fileS3Tier) indexPageFile(hash string) string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-index", hash+".json")
}

func (t *fileS3Tier) indexPendingFile() string {
	return filepath.Join(t.fs.fcfg.StoreDir, msgDir, "s3-tier.index-pending.json")
}

func (t *fileS3Tier) validIndexRef(ref s3IndexRef) bool {
	_, hashErr := hex.DecodeString(ref.SHA256)
	return len(ref.SHA256) == 64 && hashErr == nil && ref.Key == t.indexPageKey(ref.SHA256) && ref.First > 0 && ref.Last >= ref.First && ref.Count > 0 && ref.Count <= uint64(ref.Last)-uint64(ref.First)+1 && ref.Level <= 8
}

func (t *fileS3Tier) decodeIndexPage(ref s3IndexRef, data []byte, incarnation string) (s3IndexPage, error) {
	var p s3IndexPage
	if !t.validIndexRef(ref) || checksumS3Block(data) != ref.SHA256 {
		return p, errors.New("S3 tier index page checksum or reference is invalid")
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return p, err
	}
	if p.Version != 1 || p.Incarnation != incarnation || p.Level != ref.Level {
		return p, errors.New("S3 tier index page identity is invalid")
	}
	var count uint64
	var first, last uint32
	if p.Level == 0 {
		if len(p.Blocks) == 0 || len(p.Blocks) > s3IndexPageSize || len(p.Children) != 0 {
			return p, errors.New("S3 tier index leaf size is invalid")
		}
		for i, d := range p.Blocks {
			if !validS3BlockDescriptor(d) || d.Incarnation != incarnation || (i > 0 && p.Blocks[i-1].Index >= d.Index) {
				return p, errors.New("S3 tier index leaf descriptor is invalid")
			}
		}
		first, last, count = p.Blocks[0].Index, p.Blocks[len(p.Blocks)-1].Index, uint64(len(p.Blocks))
	} else {
		if len(p.Children) == 0 || len(p.Children) > s3IndexPageSize || len(p.Blocks) != 0 {
			return p, errors.New("S3 tier index branch size is invalid")
		}
		for i, c := range p.Children {
			if !t.validIndexRef(c) || c.Level+1 != p.Level || (i > 0 && p.Children[i-1].Last >= c.First) {
				return p, errors.New("S3 tier index child range is invalid")
			}
			count += c.Count
		}
		first, last = p.Children[0].First, p.Children[len(p.Children)-1].Last
	}
	if ref.First != first || ref.Last != last || ref.Count != count {
		return p, errors.New("S3 tier index page range or count is invalid")
	}
	return p, nil
}

func (t *fileS3Tier) storeIndexPage(p s3IndexPage) (s3IndexRef, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return s3IndexRef{}, err
	}
	ref := s3IndexRef{SHA256: checksumS3Block(data), Level: p.Level}
	ref.Key = t.indexPageKey(ref.SHA256)
	if p.Level == 0 {
		ref.First, ref.Last, ref.Count = p.Blocks[0].Index, p.Blocks[len(p.Blocks)-1].Index, uint64(len(p.Blocks))
	} else {
		ref.First, ref.Last = p.Children[0].First, p.Children[len(p.Children)-1].Last
		for _, c := range p.Children {
			ref.Count += c.Count
		}
	}
	if _, err = t.decodeIndexPage(ref, data, p.Incarnation); err != nil {
		return ref, err
	}
	if t.indexStaging != nil {
		t.indexStaging[ref.SHA256] = data
		return ref, nil
	}
	return ref, t.publishIndexPage(ref, data)
}

func (t *fileS3Tier) publishIndexPage(ref s3IndexRef, data []byte) error {
	if err := t.putImmutable(ref.Key, data); err != nil {
		return err
	}
	var err error
	// Local pages precede the durable root. Same-node startup never needs S3.
	dir := filepath.Dir(t.indexPageFile(ref.SHA256))
	if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
		if err = os.MkdirAll(dir, defaultDirPerms); err != nil {
			return err
		}
		if canFsyncDirectories {
			if err = syncDir(filepath.Dir(dir)); err != nil {
				return err
			}
		}
	} else if statErr != nil {
		return statErr
	}
	if err = writeAtomically(t.fs.dios, t.indexPageFile(ref.SHA256), data, defaultFilePerms, true); err != nil {
		return err
	}
	return nil
}

func (t *fileS3Tier) insertIndex(ref *s3IndexRef, d s3BlockDescriptor, obsolete *[]s3IndexRef) ([]s3IndexRef, error) {
	p := s3IndexPage{Version: 1, Incarnation: d.Incarnation}
	if ref != nil {
		data, err := t.readStagedIndexPage(ref.SHA256)
		if err != nil {
			return nil, err
		}
		p, err = t.decodeIndexPage(*ref, data, d.Incarnation)
		if err != nil {
			return nil, err
		}
	}
	if p.Level == 0 {
		i := sort.Search(len(p.Blocks), func(i int) bool { return p.Blocks[i].Index >= d.Index })
		if i < len(p.Blocks) && p.Blocks[i].Index == d.Index {
			if p.Blocks[i] != d {
				return nil, errS3TierMutation
			}
			return []s3IndexRef{*ref}, nil
		}
		p.Blocks = append(p.Blocks, s3BlockDescriptor{})
		copy(p.Blocks[i+1:], p.Blocks[i:])
		p.Blocks[i] = d
	} else {
		i := sort.Search(len(p.Children), func(i int) bool { return p.Children[i].Last >= d.Index })
		if i == len(p.Children) {
			i--
		}
		old := p.Children[i]
		children, err := t.insertIndex(&old, d, obsolete)
		if err != nil {
			return nil, err
		}
		if len(children) == 1 && children[0] == old {
			return []s3IndexRef{*ref}, nil
		}
		next := make([]s3IndexRef, 0, len(p.Children)+len(children)-1)
		next = append(next, p.Children[:i]...)
		next = append(next, children...)
		next = append(next, p.Children[i+1:]...)
		p.Children = next
	}
	if ref != nil {
		*obsolete = append(*obsolete, *ref)
	}
	n := len(p.Blocks)
	if p.Level > 0 {
		n = len(p.Children)
	}
	if n <= s3IndexPageSize {
		r, err := t.storeIndexPage(p)
		return []s3IndexRef{r}, err
	}
	right := p
	if p.Level == 0 {
		right.Blocks = p.Blocks[n/2:]
		p.Blocks = p.Blocks[:n/2]
	} else {
		right.Children = p.Children[n/2:]
		p.Children = p.Children[:n/2]
	}
	leftRef, err := t.storeIndexPage(p)
	if err != nil {
		return nil, err
	}
	rightRef, err := t.storeIndexPage(right)
	if err != nil {
		return nil, err
	}
	return []s3IndexRef{leftRef, rightRef}, nil
}

// walkManifestIndex is shared by offline local startup and pinned remote restore.
// A page's advertised count/range is checked again against its verified contents.
func (t *fileS3Tier) walkManifestIndex(m s3TierManifest, get func(s3IndexRef) ([]byte, error)) ([]s3BlockDescriptor, map[string][]byte, error) {
	if m.Version != 3 || m.Generation == 0 || m.Incarnation == "" || m.Prefix != strings.Trim(t.cfg.Prefix, "/") || (m.Root == nil) != (m.Count == 0) {
		return nil, nil, errors.New("invalid S3 tier index root")
	}
	if m.Root == nil {
		return nil, map[string][]byte{}, nil
	}
	if !t.validIndexRef(*m.Root) || m.Root.Count != m.Count {
		return nil, nil, errors.New("invalid S3 tier index root count")
	}
	pages := make(map[string][]byte)
	var blocks []s3BlockDescriptor
	var visit func(s3IndexRef) error
	visit = func(ref s3IndexRef) error {
		if _, seen := pages[ref.SHA256]; seen {
			return errors.New("S3 tier index repeats a page")
		}
		data, err := get(ref)
		if err != nil {
			return err
		}
		page, err := t.decodeIndexPage(ref, data, m.Incarnation)
		if err != nil {
			return err
		}
		pages[ref.SHA256] = data
		if page.Level == 0 {
			blocks = append(blocks, page.Blocks...)
			return nil
		}
		for _, c := range page.Children {
			if err := visit(c); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(*m.Root); err != nil {
		return nil, nil, err
	}
	if uint64(len(blocks)) != m.Count {
		return nil, nil, errors.New("S3 tier index descriptor count mismatch")
	}
	return blocks, pages, nil
}

func (t *fileS3Tier) loadManifestIndex(m s3TierManifest) ([]s3BlockDescriptor, map[string][]byte, error) {
	return t.walkManifestIndex(m, func(ref s3IndexRef) ([]byte, error) {
		ctx, cancel := t.ctx()
		defer cancel()
		return t.cfg.Store.Get(ctx, ref.Key)
	})
}

func (t *fileS3Tier) loadLocalManifestIndex(m s3TierManifest) ([]s3BlockDescriptor, error) {
	blocks, _, err := t.walkManifestIndex(m, func(ref s3IndexRef) ([]byte, error) { return os.ReadFile(t.indexPageFile(ref.SHA256)) })
	return blocks, err
}

// The prepared publication is durable before its fixed generation key is sent.
// A lost remote response or local rename can only retry those exact root bytes.
func (t *fileS3Tier) finishIndexPending() error {
	data, err := os.ReadFile(t.indexPendingFile())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var pending s3IndexPending
	if err = json.Unmarshal(data, &pending); err != nil {
		return err
	}
	m := pending.Manifest
	if m.Version != 3 || m.Incarnation != t.record.Incarnation || m.Prefix != strings.Trim(t.cfg.Prefix, "/") || m.Generation == 0 {
		return errors.New("invalid pending S3 tier index publication")
	}
	if err = t.validateIndexPending(pending); err != nil {
		return err
	}
	buf, err := json.Marshal(m)
	if err != nil {
		return err
	}
	t.mu.Lock()
	gen, hash := t.gen, t.manifestSHA256
	t.mu.Unlock()
	if m.Generation < gen || m.Generation > gen+1 || (m.Generation == gen && hash != checksumS3Block(buf)) {
		return errors.New("conflicting pending S3 tier index publication")
	}
	if m.Generation > gen {
		if err = t.putImmutable(t.manifestKey(m.Generation), buf); err != nil {
			return err
		}
		if err = writeAtomically(t.fs.dios, t.manifestFile(), buf, defaultFilePerms, true); err != nil {
			return err
		}
		t.mu.Lock()
		t.gen, t.manifestSHA256, t.indexRoot = m.Generation, checksumS3Block(buf), m.Root
		t.mu.Unlock()
	}
	for _, d := range pending.Added {
		if !validS3BlockDescriptor(d) || d.Incarnation != m.Incarnation {
			return errors.New("invalid pending S3 tier descriptor")
		}
		bytes, err := json.Marshal(d)
		if err != nil {
			return err
		}
		if err = writeAtomically(t.fs.dios, t.descriptorFile(d.Index), bytes, defaultFilePerms, true); err != nil {
			return err
		}
		t.mu.Lock()
		t.desc[d.Index] = d
		t.mu.Unlock()
	}
	for _, old := range pending.Obsolete {
		data, err := os.ReadFile(t.indexPageFile(old.SHA256))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if _, err = t.decodeIndexPage(old, data, m.Incarnation); err != nil {
			return err
		}
		retained, err := t.indexPathContains(m.Root, old)
		if err != nil {
			return err
		}
		if retained {
			return errors.New("pending S3 index cleanup references a retained page")
		}
		path := t.indexPageFile(old.SHA256)
		if err = os.Remove(path); err != nil {
			return err
		}
		t.fs.dios.budget.Load().completed(path)
	}
	if len(pending.Obsolete) > 0 && canFsyncDirectories {
		if err = syncDir(filepath.Dir(t.indexPageFile(""))); err != nil {
			return err
		}
	}
	if err = os.Remove(t.indexPendingFile()); err != nil && !os.IsNotExist(err) {
		return err
	}
	t.fs.dios.budget.Load().completed(t.indexPendingFile())
	if canFsyncDirectories {
		return syncDir(filepath.Dir(t.indexPendingFile()))
	}
	return nil
}

// Validate only the changed paths during publication. Full-tree validation is
// reserved for startup/restore, so committing one block never scans all history.
func (t *fileS3Tier) validateIndexPending(p s3IndexPending) error {
	m := p.Manifest
	if (m.Root == nil) != (m.Count == 0) || (m.Root != nil && (!t.validIndexRef(*m.Root) || m.Root.Count != m.Count)) {
		return errors.New("invalid pending S3 tier root")
	}
	for _, d := range p.Added {
		if !validS3BlockDescriptor(d) || d.Incarnation != m.Incarnation {
			return errors.New("invalid pending S3 tier descriptor")
		}
		found, err := t.findIndexDescriptor(m.Root, d.Index, m.Incarnation)
		if err != nil {
			return err
		}
		if found == nil || *found != d {
			return errors.New("pending S3 tier descriptor is absent from index")
		}
	}
	for _, old := range p.Obsolete {
		if !t.validIndexRef(old) {
			return errors.New("invalid pending S3 tier obsolete page")
		}
	}
	return nil
}

func (t *fileS3Tier) findIndexDescriptor(ref *s3IndexRef, index uint32, incarnation string) (*s3BlockDescriptor, error) {
	for ref != nil {
		if !t.validIndexRef(*ref) {
			return nil, errors.New("invalid S3 tier lookup reference")
		}
		if index < ref.First || index > ref.Last {
			return nil, nil
		}
		data, err := os.ReadFile(t.indexPageFile(ref.SHA256))
		if err != nil {
			return nil, err
		}
		p, err := t.decodeIndexPage(*ref, data, incarnation)
		if err != nil {
			return nil, err
		}
		if p.Level == 0 {
			i := sort.Search(len(p.Blocks), func(i int) bool { return p.Blocks[i].Index >= index })
			if i < len(p.Blocks) && p.Blocks[i].Index == index {
				return &p.Blocks[i], nil
			}
			return nil, nil
		}
		i := sort.Search(len(p.Children), func(i int) bool { return p.Children[i].Last >= index })
		if i == len(p.Children) {
			return nil, nil
		}
		ref = &p.Children[i]
	}
	return nil, nil
}

func (t *fileS3Tier) indexPathContains(root *s3IndexRef, old s3IndexRef) (bool, error) {
	for root != nil && root.Level >= old.Level && root.First <= old.First && root.Last >= old.First {
		if root.SHA256 == old.SHA256 {
			return true, nil
		}
		if root.Level == old.Level {
			return false, nil
		}
		data, err := os.ReadFile(t.indexPageFile(root.SHA256))
		if err != nil {
			return false, err
		}
		p, err := t.decodeIndexPage(*root, data, t.record.Incarnation)
		if err != nil {
			return false, err
		}
		i := sort.Search(len(p.Children), func(i int) bool { return p.Children[i].Last >= old.First })
		if i == len(p.Children) {
			return false, nil
		}
		root = &p.Children[i]
	}
	return false, nil
}

// A crash before the pending journal can leave unreachable local page files.
// Retain both committed and prepared roots; old remote checkpoints remain in S3.
func (t *fileS3Tier) recoverIndexCache() error {
	keep := make(map[string]bool)
	for _, path := range []string{t.manifestFile(), t.indexPendingFile()} {
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		var m s3TierManifest
		if path == t.indexPendingFile() {
			var p s3IndexPending
			if err = json.Unmarshal(data, &p); err != nil {
				return err
			}
			m = p.Manifest
			if t.record == nil || m.Incarnation != t.record.Incarnation {
				return errors.New("pending S3 tier index identity mismatch")
			}
			if err = t.validateIndexPending(p); err != nil {
				return err
			}
		} else if err = json.Unmarshal(data, &m); err != nil {
			return err
		}
		_, pages, err := t.walkManifestIndex(m, func(ref s3IndexRef) ([]byte, error) { return os.ReadFile(t.indexPageFile(ref.SHA256)) })
		if err != nil {
			return err
		}
		for hash := range pages {
			keep[hash+".json"] = true
		}
	}
	dir := filepath.Dir(t.indexPageFile(""))
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || keep[entry.Name()] {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err = os.Remove(path); err != nil {
			return err
		}
		t.fs.dios.budget.Load().completed(path)
	}
	if canFsyncDirectories {
		return syncDir(dir)
	}
	return nil
}

func (t *fileS3Tier) readStagedIndexPage(hash string) ([]byte, error) {
	if data, ok := t.indexStaging[hash]; ok {
		return data, nil
	}
	return os.ReadFile(t.indexPageFile(hash))
}

// Intermediate versions within a four-block batch never become objects. Write
// just the final changed paths, children before parents, before publishing root.
func (t *fileS3Tier) publishStagedIndex(ref *s3IndexRef) error {
	if ref == nil {
		return nil
	}
	data, ok := t.indexStaging[ref.SHA256]
	if !ok {
		return nil
	}
	page, err := t.decodeIndexPage(*ref, data, t.record.Incarnation)
	if err != nil {
		return err
	}
	for _, child := range page.Children {
		if err = t.publishStagedIndex(&child); err != nil {
			return err
		}
	}
	return t.publishIndexPage(*ref, data)
}

type s3TierCoverage struct {
	block      *msgBlock
	data       []byte
	descriptor s3BlockDescriptor
}
