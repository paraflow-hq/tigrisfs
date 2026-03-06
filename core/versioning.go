// Copyright 2024 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tigrisdata/tigrisfs/log"
)

var versionLog = log.GetLogger("versioning")

// pendingSnapshot stores the old file content captured at MultipartBlobBegin
// time so it can be snapshotted when the multipart upload commits.
type pendingSnapshot struct {
	uuid    string
	key     string
	content []byte
	size    uint64
}

// VersioningBackend wraps any StorageBackend and intercepts key write
// operations to create version snapshots. Versions are stored under
// .versions/{uuid}/ within the same S3 bucket.
//
// Each version snapshot object has custom metadata:
//   - fileuuid: the file's UUID
//   - filepath: the file path at the time of the snapshot
//   - version: the version number (string)
//   - snapshot-timestamp: RFC3339 timestamp of the snapshot
//   - deleted: "true" if this snapshot was taken because the file was deleted
type VersioningBackend struct {
	backend          StorageBackend
	versionPrefix    string // default: ".versions/"
	enabled          bool
	mu               sync.Mutex
	pendingMultipart map[string]*pendingSnapshot
}

// NewVersioningBackend creates a VersioningBackend that wraps the given
// backend. If versionPrefix is empty it defaults to ".versions/".
func NewVersioningBackend(backend StorageBackend, versionPrefix string) *VersioningBackend {
	if versionPrefix == "" {
		versionPrefix = ".versions/"
	}
	return &VersioningBackend{
		backend:          backend,
		versionPrefix:    versionPrefix,
		enabled:          true,
		pendingMultipart: make(map[string]*pendingSnapshot),
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (vb *VersioningBackend) isVersionPath(key string) bool {
	return strings.HasPrefix(key, vb.versionPrefix)
}

// nextVersionNumber lists existing snapshots for a UUID and returns the next
// version number (max existing + 1, or 1 if none exist).
func (vb *VersioningBackend) nextVersionNumber(fileUUID string) (int, error) {
	prefix := vb.versionPrefix + fileUUID + "/"
	resp, err := vb.backend.ListBlobs(&ListBlobsInput{Prefix: &prefix})
	if err != nil {
		return 1, nil // treat list failure as empty
	}

	maxVersion := 0
	for _, item := range resp.Items {
		if item.Key == nil {
			continue
		}
		// Extract version number from key: .versions/{uuid}/{version}
		suffix := strings.TrimPrefix(*item.Key, prefix)
		if v, err := strconv.Atoi(suffix); err == nil && v > maxVersion {
			maxVersion = v
		}
	}
	return maxVersion + 1, nil
}

// snapshotVersion writes the content as the next version snapshot with all
// metadata stored on the snapshot object itself. Returns the new version number.
func (vb *VersioningBackend) snapshotVersion(key string, fileUUID string, body io.ReadSeeker, size uint64, deleted bool) (int, error) {
	nextVersion, err := vb.nextVersionNumber(fileUUID)
	if err != nil {
		return 0, err
	}

	snapshotKey := vb.versionPrefix + fileUUID + "/" + strconv.Itoa(nextVersion)
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("versioning: seek failed for snapshot %s v%d: %w", fileUUID, nextVersion, err)
	}

	// Store all metadata on the snapshot object itself.
	versionStr := strconv.Itoa(nextVersion)
	timestamp := time.Now().UTC().Format(time.RFC3339)
	meta := map[string]*string{
		"fileuuid":           &fileUUID,
		"filepath":           &key,
		"version":            &versionStr,
		"snapshot-timestamp": &timestamp,
	}
	if deleted {
		trueStr := "true"
		meta["deleted"] = &trueStr
	}

	_, err = vb.backend.PutBlob(&PutBlobInput{
		Key:      snapshotKey,
		Body:     body,
		Size:     &size,
		Metadata: meta,
	})
	if err != nil {
		return 0, fmt.Errorf("versioning: failed to write snapshot %s v%d: %w", fileUUID, nextVersion, err)
	}

	versionLog.Debugf("snapshotVersion: %s uuid=%s version=%d size=%d deleted=%v", key, fileUUID, nextVersion, size, deleted)
	return nextVersion, nil
}

// getOrCreateUUID extracts the fileuuid from existing metadata, or generates a
// new one. It also ensures the metadata map contains the fileuuid key.
func (vb *VersioningBackend) getOrCreateUUID(metadata map[string]*string) (string, map[string]*string) {
	if metadata == nil {
		metadata = make(map[string]*string)
	}
	if v, ok := metadata["fileuuid"]; ok && v != nil && *v != "" {
		return *v, metadata
	}
	newUUID := uuid.New().String()
	metadata["fileuuid"] = &newUUID
	return newUUID, metadata
}

// readFullBlob reads the entire content of a key via GetBlob.
func (vb *VersioningBackend) readFullBlob(key string) ([]byte, map[string]*string, uint64, error) {
	resp, err := vb.backend.GetBlob(&GetBlobInput{Key: key})
	if err != nil {
		return nil, nil, 0, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("versioning: failed to read blob %s: %w", key, err)
	}
	return data, resp.Metadata, resp.Size, nil
}

// ListVersions returns the snapshot keys for a UUID, sorted by version number.
func (vb *VersioningBackend) ListVersions(fileUUID string) ([]string, error) {
	prefix := vb.versionPrefix + fileUUID + "/"
	resp, err := vb.backend.ListBlobs(&ListBlobsInput{Prefix: &prefix})
	if err != nil {
		return nil, err
	}

	type versionKey struct {
		key     string
		version int
	}
	var versions []versionKey
	for _, item := range resp.Items {
		if item.Key == nil {
			continue
		}
		suffix := strings.TrimPrefix(*item.Key, prefix)
		if v, err := strconv.Atoi(suffix); err == nil {
			versions = append(versions, versionKey{key: *item.Key, version: v})
		}
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].version < versions[j].version
	})

	keys := make([]string, len(versions))
	for i, v := range versions {
		keys[i] = v.key
	}
	return keys, nil
}

// ---------------------------------------------------------------------------
// StorageBackend interface -- pass-through methods
// ---------------------------------------------------------------------------

func (vb *VersioningBackend) Init(key string) error {
	return vb.backend.Init(key)
}

func (vb *VersioningBackend) Capabilities() *Capabilities {
	return vb.backend.Capabilities()
}

func (vb *VersioningBackend) Bucket() string {
	return vb.backend.Bucket()
}

func (vb *VersioningBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	return vb.backend.HeadBlob(param)
}

func (vb *VersioningBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return vb.backend.RenameBlob(param)
}

func (vb *VersioningBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	return vb.backend.GetBlob(param)
}

func (vb *VersioningBackend) PatchBlob(param *PatchBlobInput) (*PatchBlobOutput, error) {
	return vb.backend.PatchBlob(param)
}

func (vb *VersioningBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return vb.backend.MultipartBlobAdd(param)
}

func (vb *VersioningBackend) MultipartBlobCopy(param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error) {
	return vb.backend.MultipartBlobCopy(param)
}

func (vb *VersioningBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return vb.backend.MultipartExpire(param)
}

func (vb *VersioningBackend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	return vb.backend.RemoveBucket(param)
}

func (vb *VersioningBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	return vb.backend.MakeBucket(param)
}

func (vb *VersioningBackend) Delegate() interface{} {
	return vb.backend
}

// ---------------------------------------------------------------------------
// StorageBackend interface -- overridden methods
// ---------------------------------------------------------------------------

// ListBlobs delegates to the backend and filters out entries whose key or
// prefix starts with the version prefix so that version data is invisible
// to the rest of the filesystem.
func (vb *VersioningBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	out, err := vb.backend.ListBlobs(param)
	if err != nil || !vb.enabled {
		return out, err
	}

	// Filter items.
	filtered := out.Items[:0]
	for _, item := range out.Items {
		if item.Key != nil && vb.isVersionPath(*item.Key) {
			continue
		}
		filtered = append(filtered, item)
	}
	out.Items = filtered

	// Filter prefixes.
	filteredPrefixes := out.Prefixes[:0]
	for _, pfx := range out.Prefixes {
		if pfx.Prefix != nil && vb.isVersionPath(*pfx.Prefix) {
			continue
		}
		filteredPrefixes = append(filteredPrefixes, pfx)
	}
	out.Prefixes = filteredPrefixes

	return out, nil
}

// PutBlob intercepts writes to snapshot the previous version before the new
// content is written. A fileuuid is maintained in object metadata.
func (vb *VersioningBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	if !vb.enabled || vb.isVersionPath(param.Key) {
		return vb.backend.PutBlob(param)
	}

	// Check whether the file already exists so we can snapshot the old content.
	existingData, existingMeta, existingSize, getErr := vb.readFullBlob(param.Key)

	var fileUUID string
	if getErr == nil {
		// File exists -- extract UUID from existing metadata.
		if v, ok := existingMeta["fileuuid"]; ok && v != nil && *v != "" {
			fileUUID = *v
		}
	}

	// If we still have no UUID, check the incoming metadata or generate one.
	if fileUUID == "" {
		fileUUID, param.Metadata = vb.getOrCreateUUID(param.Metadata)
	} else {
		// Ensure the new write carries the UUID forward.
		if param.Metadata == nil {
			param.Metadata = make(map[string]*string)
		}
		param.Metadata["fileuuid"] = &fileUUID
	}

	// Snapshot the old content before overwriting.
	if getErr == nil {
		if _, snapErr := vb.snapshotVersion(param.Key, fileUUID, bytes.NewReader(existingData), existingSize, false); snapErr != nil {
			versionLog.Errorf("PutBlob: snapshot failed for %s: %v", param.Key, snapErr)
		}
	}

	return vb.backend.PutBlob(param)
}

// DeleteBlob snapshots the final version before deleting the object. The
// snapshot's metadata includes deleted=true so consumers know the file was
// deleted after this version.
func (vb *VersioningBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	if !vb.enabled || vb.isVersionPath(param.Key) {
		return vb.backend.DeleteBlob(param)
	}

	// Read the existing file to get UUID and content for final snapshot.
	data, meta, size, getErr := vb.readFullBlob(param.Key)
	if getErr == nil {
		fileUUID := ""
		if v, ok := meta["fileuuid"]; ok && v != nil {
			fileUUID = *v
		}
		if fileUUID != "" {
			if _, snapErr := vb.snapshotVersion(param.Key, fileUUID, bytes.NewReader(data), size, true); snapErr != nil {
				versionLog.Errorf("DeleteBlob: snapshot failed for %s: %v", param.Key, snapErr)
			}
		}
	}

	return vb.backend.DeleteBlob(param)
}

// DeleteBlobs calls DeleteBlob individually for keys that require versioning,
// then delegates the rest in bulk.
func (vb *VersioningBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	if !vb.enabled {
		return vb.backend.DeleteBlobs(param)
	}

	var bulk []string
	for _, key := range param.Items {
		if vb.isVersionPath(key) {
			// Version-path items don't need versioning; delete in bulk.
			bulk = append(bulk, key)
			continue
		}
		// Use single-item delete so versioning logic runs.
		if _, err := vb.DeleteBlob(&DeleteBlobInput{Key: key}); err != nil {
			versionLog.Errorf("DeleteBlobs: failed to delete %s: %v", key, err)
		}
	}

	// Delete version-path items in bulk (no versioning needed).
	if len(bulk) > 0 {
		return vb.backend.DeleteBlobs(&DeleteBlobsInput{Items: bulk})
	}
	return &DeleteBlobsOutput{}, nil
}

// CopyBlob is used for rename. It preserves the UUID across the copy. No
// content snapshot is created because a rename is not a content change.
func (vb *VersioningBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	if !vb.enabled || vb.isVersionPath(param.Source) || vb.isVersionPath(param.Destination) {
		return vb.backend.CopyBlob(param)
	}

	// Get UUID from the source file.
	headOut, headErr := vb.backend.HeadBlob(&HeadBlobInput{Key: param.Source})
	var fileUUID string
	if headErr == nil && headOut.Metadata != nil {
		if v, ok := headOut.Metadata["fileuuid"]; ok && v != nil {
			fileUUID = *v
		}
	}

	if fileUUID != "" {
		// Inject UUID into destination metadata so it survives the copy.
		// Always start from source metadata, then merge any caller-provided
		// metadata on top, so we never lose existing keys.
		merged := make(map[string]*string)
		if headOut.Metadata != nil {
			for k, v := range headOut.Metadata {
				merged[k] = v
			}
		}
		for k, v := range param.Metadata { // no-op when param.Metadata is nil
			merged[k] = v
		}
		merged["fileuuid"] = &fileUUID
		param.Metadata = merged
	}

	return vb.backend.CopyBlob(param)
}

// MultipartBlobBegin checks if the target file already exists, captures its
// content for a pending snapshot, injects the UUID into metadata, and then
// delegates to the backend.
func (vb *VersioningBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	if !vb.enabled || vb.isVersionPath(param.Key) {
		return vb.backend.MultipartBlobBegin(param)
	}

	// Read existing file (if any) to capture for later snapshot.
	var snap *pendingSnapshot
	data, meta, size, getErr := vb.readFullBlob(param.Key)
	if getErr == nil {
		fileUUID := ""
		if v, ok := meta["fileuuid"]; ok && v != nil && *v != "" {
			fileUUID = *v
		}
		if fileUUID != "" {
			snap = &pendingSnapshot{
				uuid:    fileUUID,
				key:     param.Key,
				content: data,
				size:    size,
			}
		}
	}

	// Get or create UUID for the new upload.
	var fileUUID string
	if snap != nil {
		fileUUID = snap.uuid
	} else {
		fileUUID, param.Metadata = vb.getOrCreateUUID(param.Metadata)
	}

	// Ensure metadata carries UUID.
	if param.Metadata == nil {
		param.Metadata = make(map[string]*string)
	}
	param.Metadata["fileuuid"] = &fileUUID

	commitInput, err := vb.backend.MultipartBlobBegin(param)
	if err != nil {
		return commitInput, err
	}

	// Store pending snapshot keyed by upload ID.
	if snap != nil && commitInput.UploadId != nil {
		vb.mu.Lock()
		vb.pendingMultipart[*commitInput.UploadId] = snap
		vb.mu.Unlock()
	}

	return commitInput, nil
}

// MultipartBlobCommit finalises the multipart upload and then creates the
// version snapshot from the content captured at begin time.
func (vb *VersioningBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	if !vb.enabled {
		return vb.backend.MultipartBlobCommit(param)
	}

	out, err := vb.backend.MultipartBlobCommit(param)
	if err != nil {
		return out, err
	}

	// Create snapshot from pending data.
	if param.UploadId != nil {
		vb.mu.Lock()
		snap, ok := vb.pendingMultipart[*param.UploadId]
		if ok {
			delete(vb.pendingMultipart, *param.UploadId)
		}
		vb.mu.Unlock()

		if ok && snap != nil && param.Key != nil {
			if _, snapErr := vb.snapshotVersion(snap.key, snap.uuid, bytes.NewReader(snap.content), snap.size, false); snapErr != nil {
				versionLog.Errorf("MultipartBlobCommit: snapshot failed for %s: %v", *param.Key, snapErr)
			}
		}
	}

	return out, nil
}

// MultipartBlobAbort cleans up the pending snapshot entry and delegates to
// the backend.
func (vb *VersioningBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	out, err := vb.backend.MultipartBlobAbort(param)
	if err == nil && param.UploadId != nil {
		vb.mu.Lock()
		delete(vb.pendingMultipart, *param.UploadId)
		vb.mu.Unlock()
	}
	return out, err
}
