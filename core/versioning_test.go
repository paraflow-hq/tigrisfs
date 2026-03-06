package core

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigrisfs/core/cfg"
)

// newR2Backend creates an S3Backend connected to R2 using environment variables.
// It skips the test if credentials are not set.
func newR2Backend(t *testing.T) StorageBackend {
	t.Helper()

	accessKey := os.Getenv("R2_ACCESS_KEY_ID")
	secretKey := os.Getenv("R2_SECRET_ACCESS_KEY")
	endpoint := os.Getenv("R2_ENDPOINT")
	bucket := os.Getenv("R2_BUCKET")

	if accessKey == "" || secretKey == "" || endpoint == "" || bucket == "" {
		t.Skip("R2 credentials not set (R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, R2_BUCKET)")
	}

	flags := &cfg.FlagStorage{
		Endpoint: endpoint,
	}
	s3conf := &cfg.S3Config{
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		Region:     "auto",
		RegionSet:  true,
		NoDetect:   true,
		NoChecksum: true,
	}
	s3conf.Init()

	s3, err := NewS3(bucket, flags, s3conf)
	require.NoError(t, err)

	return s3
}

// testPrefix returns a unique prefix for test isolation.
func testPrefix(t *testing.T) string {
	return fmt.Sprintf("_test_versioning/%s/%d/", t.Name(), time.Now().UnixNano())
}

// cleanupPrefix deletes all objects under a prefix.
func cleanupPrefix(t *testing.T, backend StorageBackend, prefix string) {
	t.Helper()
	for {
		resp, err := backend.ListBlobs(&ListBlobsInput{
			Prefix: &prefix,
		})
		if err != nil {
			return
		}
		if len(resp.Items) == 0 {
			break
		}
		for _, item := range resp.Items {
			if item.Key != nil {
				_, _ = backend.DeleteBlob(&DeleteBlobInput{Key: *item.Key})
			}
		}
		if !resp.IsTruncated {
			break
		}
	}
}

// putTestBlob is a helper that writes a blob with content and optional metadata.
func putTestBlob(t *testing.T, backend StorageBackend, key string, content string, metadata map[string]*string) {
	t.Helper()
	data := []byte(content)
	size := uint64(len(data))
	_, err := backend.PutBlob(&PutBlobInput{
		Key:      key,
		Body:     bytes.NewReader(data),
		Size:     &size,
		Metadata: metadata,
	})
	require.NoError(t, err)
}

// readTestBlob reads a blob's body as string.
func readTestBlob(t *testing.T, backend StorageBackend, key string) string {
	t.Helper()
	resp, err := backend.GetBlob(&GetBlobInput{Key: key})
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(data)
}

// listVersionKeys lists snapshot keys for a UUID, returning them sorted by version.
func listVersionKeys(t *testing.T, backend StorageBackend, versionPrefix, uuid string) []string {
	t.Helper()
	prefix := versionPrefix + uuid + "/"
	resp, err := backend.ListBlobs(&ListBlobsInput{Prefix: &prefix})
	require.NoError(t, err)

	var keys []string
	for _, item := range resp.Items {
		if item.Key != nil {
			suffix := strings.TrimPrefix(*item.Key, prefix)
			if _, err := strconv.Atoi(suffix); err == nil {
				keys = append(keys, *item.Key)
			}
		}
	}
	return keys
}

// getSnapshotMetadata reads the metadata from a snapshot object.
func getSnapshotMetadata(t *testing.T, backend StorageBackend, key string) map[string]*string {
	t.Helper()
	head, err := backend.HeadBlob(&HeadBlobInput{Key: key})
	require.NoError(t, err)
	return head.Metadata
}

func TestVersioningPutBlob(t *testing.T) {
	raw := newR2Backend(t)
	prefix := testPrefix(t)
	versionPrefix := prefix + ".versions/"
	vb := NewVersioningBackend(raw, versionPrefix)
	defer cleanupPrefix(t, raw, prefix)

	key := prefix + "hello.txt"

	// First write — new file, no previous version to snapshot
	putTestBlob(t, vb, key, "version 1", nil)

	// Verify file exists with UUID in metadata
	head, err := raw.HeadBlob(&HeadBlobInput{Key: key})
	require.NoError(t, err)
	require.NotNil(t, head.Metadata)
	uuid1 := head.Metadata["fileuuid"]
	require.NotNil(t, uuid1)
	require.NotEmpty(t, *uuid1)
	t.Logf("Assigned UUID: %s", *uuid1)

	// Second write — should snapshot "version 1"
	putTestBlob(t, vb, key, "version 2", nil)

	// Verify UUID is preserved
	head2, err := raw.HeadBlob(&HeadBlobInput{Key: key})
	require.NoError(t, err)
	uuid2 := head2.Metadata["fileuuid"]
	require.NotNil(t, uuid2)
	require.Equal(t, *uuid1, *uuid2, "UUID should be preserved across writes")

	// Verify snapshots exist via ListBlobs
	snapKeys := listVersionKeys(t, raw, versionPrefix, *uuid1)
	require.NotEmpty(t, snapKeys, "Should have at least one snapshot")

	// Check that at least one snapshot contains "version 1"
	found := false
	for _, snapKey := range snapKeys {
		content := readTestBlob(t, raw, snapKey)
		meta := getSnapshotMetadata(t, raw, snapKey)
		t.Logf("Snapshot %s: content=%q filepath=%v", snapKey, content, meta["filepath"])
		if content == "version 1" {
			found = true
			// Verify snapshot metadata
			require.NotNil(t, meta["fileuuid"])
			require.Equal(t, *uuid1, *meta["fileuuid"])
			require.NotNil(t, meta["filepath"])
			require.Equal(t, key, *meta["filepath"])
			require.NotNil(t, meta["version"])
			require.NotNil(t, meta["snapshot-timestamp"])
		}
	}
	require.True(t, found, "Should have a snapshot of 'version 1'")

	// Third write
	putTestBlob(t, vb, key, "version 3", nil)

	// Verify "version 2" is now also snapshotted
	snapKeys = listVersionKeys(t, raw, versionPrefix, *uuid1)
	found = false
	for _, snapKey := range snapKeys {
		content := readTestBlob(t, raw, snapKey)
		if content == "version 2" {
			found = true
		}
	}
	require.True(t, found, "Should have a snapshot of 'version 2'")

	// Current content should be "version 3"
	content := readTestBlob(t, vb, key)
	require.Equal(t, "version 3", content)

	t.Log("PASS: PutBlob versioning works correctly")
}

func TestVersioningCopyBlob(t *testing.T) {
	raw := newR2Backend(t)
	prefix := testPrefix(t)
	versionPrefix := prefix + ".versions/"
	vb := NewVersioningBackend(raw, versionPrefix)
	defer cleanupPrefix(t, raw, prefix)

	srcKey := prefix + "original.txt"
	dstKey := prefix + "renamed.txt"

	// Write two versions so there's version history
	putTestBlob(t, vb, srcKey, "content v1", nil)
	putTestBlob(t, vb, srcKey, "content v2", nil)

	// Get UUID
	head, err := raw.HeadBlob(&HeadBlobInput{Key: srcKey})
	require.NoError(t, err)
	fileUUID := *head.Metadata["fileuuid"]

	// Copy (rename)
	_, err = vb.CopyBlob(&CopyBlobInput{
		Source:      srcKey,
		Destination: dstKey,
	})
	require.NoError(t, err)

	// Verify UUID is preserved on destination
	headDst, err := raw.HeadBlob(&HeadBlobInput{Key: dstKey})
	require.NoError(t, err)
	require.Equal(t, fileUUID, *headDst.Metadata["fileuuid"], "UUID should be preserved after copy/rename")

	// Content should be readable at destination
	content := readTestBlob(t, vb, dstKey)
	require.Equal(t, "content v2", content)

	// Version snapshots should still exist and reference the original path
	snapKeys := listVersionKeys(t, raw, versionPrefix, fileUUID)
	require.NotEmpty(t, snapKeys, "Version snapshots should still exist after rename")

	t.Log("PASS: CopyBlob (rename) preserves UUID")
}

func TestVersioningDeleteBlob(t *testing.T) {
	raw := newR2Backend(t)
	prefix := testPrefix(t)
	versionPrefix := prefix + ".versions/"
	vb := NewVersioningBackend(raw, versionPrefix)
	defer cleanupPrefix(t, raw, prefix)

	key := prefix + "to-delete.txt"

	// Write two versions
	putTestBlob(t, vb, key, "delete v1", nil)
	putTestBlob(t, vb, key, "delete v2", nil)

	head, err := raw.HeadBlob(&HeadBlobInput{Key: key})
	require.NoError(t, err)
	fileUUID := *head.Metadata["fileuuid"]

	// Delete
	_, err = vb.DeleteBlob(&DeleteBlobInput{Key: key})
	require.NoError(t, err)

	// File should be gone
	_, err = raw.HeadBlob(&HeadBlobInput{Key: key})
	require.Error(t, err, "File should be deleted")

	// Version snapshots should still exist
	snapKeys := listVersionKeys(t, raw, versionPrefix, fileUUID)
	require.NotEmpty(t, snapKeys, "Version snapshots should survive file deletion")

	// The last snapshot should have deleted=true and contain "delete v2"
	lastKey := snapKeys[len(snapKeys)-1]
	content := readTestBlob(t, raw, lastKey)
	require.Equal(t, "delete v2", content, "Last snapshot should be the final content before delete")

	meta := getSnapshotMetadata(t, raw, lastKey)
	require.NotNil(t, meta["deleted"])
	require.Equal(t, "true", *meta["deleted"], "Last snapshot should be marked as deleted")
	require.NotNil(t, meta["filepath"])
	require.Equal(t, key, *meta["filepath"], "Snapshot filepath should match the deleted file's path")

	t.Log("PASS: DeleteBlob creates final snapshot with deleted=true")
}

func TestVersioningListBlobsFiltering(t *testing.T) {
	raw := newR2Backend(t)
	prefix := testPrefix(t)
	versionPrefix := prefix + ".versions/"
	vb := NewVersioningBackend(raw, versionPrefix)
	defer cleanupPrefix(t, raw, prefix)

	key := prefix + "visible.txt"

	// Write with versioning to create .versions/ entries
	putTestBlob(t, vb, key, "v1", nil)
	putTestBlob(t, vb, key, "v2", nil)

	// List via raw backend — should see .versions/ entries
	pfx := prefix
	rawList, err := raw.ListBlobs(&ListBlobsInput{Prefix: &pfx})
	require.NoError(t, err)
	hasVersionKey := false
	for _, item := range rawList.Items {
		if item.Key != nil && strings.Contains(*item.Key, ".versions/") {
			hasVersionKey = true
			break
		}
	}
	require.True(t, hasVersionKey, "Raw backend should list .versions/ keys")

	// List via versioning backend — .versions/ should be filtered out
	vbList, err := vb.ListBlobs(&ListBlobsInput{Prefix: &pfx})
	require.NoError(t, err)
	for _, item := range vbList.Items {
		if item.Key != nil {
			require.False(t, strings.Contains(*item.Key, ".versions/"),
				"VersioningBackend.ListBlobs should filter out .versions/ keys, got: %s", *item.Key)
		}
	}

	// The visible file should still be in the list
	foundVisible := false
	for _, item := range vbList.Items {
		if item.Key != nil && *item.Key == key {
			foundVisible = true
		}
	}
	require.True(t, foundVisible, "Should still list the regular file")

	t.Log("PASS: ListBlobs correctly filters out .versions/ entries")
}

func TestVersioningFullLifecycle(t *testing.T) {
	raw := newR2Backend(t)
	prefix := testPrefix(t)
	versionPrefix := prefix + ".versions/"
	vb := NewVersioningBackend(raw, versionPrefix)
	defer cleanupPrefix(t, raw, prefix)

	// 1. Create a file
	key := prefix + "lifecycle.txt"
	putTestBlob(t, vb, key, "initial content", nil)

	head, err := raw.HeadBlob(&HeadBlobInput{Key: key})
	require.NoError(t, err)
	fileUUID := *head.Metadata["fileuuid"]
	t.Logf("Step 1 - Created file with UUID: %s", fileUUID)

	// 2. Edit the file 3 times
	putTestBlob(t, vb, key, "edit 1", nil)
	putTestBlob(t, vb, key, "edit 2", nil)
	putTestBlob(t, vb, key, "edit 3", nil)

	// 3. Verify version history
	snapKeys := listVersionKeys(t, raw, versionPrefix, fileUUID)
	t.Logf("Step 3 - Snapshot count: %d", len(snapKeys))
	require.GreaterOrEqual(t, len(snapKeys), 3, "Should have at least 3 version snapshots")

	// Collect all snapshot contents and verify metadata
	snapContents := make(map[string]bool)
	for _, snapKey := range snapKeys {
		content := readTestBlob(t, raw, snapKey)
		meta := getSnapshotMetadata(t, raw, snapKey)
		snapContents[content] = true
		t.Logf("  %s: %q (filepath=%v)", snapKey, content, *meta["filepath"])

		// Every snapshot should have complete metadata
		require.NotNil(t, meta["fileuuid"], "snapshot should have fileuuid")
		require.Equal(t, fileUUID, *meta["fileuuid"])
		require.NotNil(t, meta["filepath"], "snapshot should have filepath")
		require.NotNil(t, meta["version"], "snapshot should have version")
		require.NotNil(t, meta["snapshot-timestamp"], "snapshot should have snapshot-timestamp")
	}
	require.True(t, snapContents["initial content"], "Should have snapshot of 'initial content'")
	require.True(t, snapContents["edit 1"], "Should have snapshot of 'edit 1'")
	require.True(t, snapContents["edit 2"], "Should have snapshot of 'edit 2'")

	// 4. Rename the file
	newKey := prefix + "lifecycle_renamed.txt"
	_, err = vb.CopyBlob(&CopyBlobInput{Source: key, Destination: newKey})
	require.NoError(t, err)

	// Verify UUID preserved
	headNew, err := raw.HeadBlob(&HeadBlobInput{Key: newKey})
	require.NoError(t, err)
	require.Equal(t, fileUUID, *headNew.Metadata["fileuuid"])
	t.Log("Step 4 - Renamed, UUID preserved")

	// 5. Edit after rename
	putTestBlob(t, vb, newKey, "after rename", nil)

	// UUID should still be the same
	headAfter, err := raw.HeadBlob(&HeadBlobInput{Key: newKey})
	require.NoError(t, err)
	require.Equal(t, fileUUID, *headAfter.Metadata["fileuuid"])

	// "edit 3" should now be snapshotted
	snapKeys = listVersionKeys(t, raw, versionPrefix, fileUUID)
	snapContents = make(map[string]bool)
	for _, snapKey := range snapKeys {
		content := readTestBlob(t, raw, snapKey)
		snapContents[content] = true
	}
	require.True(t, snapContents["edit 3"], "Should have snapshot of 'edit 3' after rename+write")

	// The snapshot of "edit 3" should have the new path in its filepath metadata
	for _, snapKey := range snapKeys {
		content := readTestBlob(t, raw, snapKey)
		if content == "edit 3" {
			meta := getSnapshotMetadata(t, raw, snapKey)
			// The snapshot was taken when writing "after rename" to newKey,
			// but the snapshotted content was at newKey (edit 3 was the content before this write).
			// filepath should be newKey since that's where the file was when snapshotted.
			require.Equal(t, newKey, *meta["filepath"])
		}
	}

	// 6. Delete the file
	_, err = vb.DeleteBlob(&DeleteBlobInput{Key: newKey})
	require.NoError(t, err)

	snapKeys = listVersionKeys(t, raw, versionPrefix, fileUUID)
	require.GreaterOrEqual(t, len(snapKeys), 5, "Should have at least 5 snapshots total")

	// Last snapshot should be "after rename" with deleted=true
	lastKey := snapKeys[len(snapKeys)-1]
	content := readTestBlob(t, raw, lastKey)
	require.Equal(t, "after rename", content, "Last snapshot should be the final content before delete")

	lastMeta := getSnapshotMetadata(t, raw, lastKey)
	require.NotNil(t, lastMeta["deleted"])
	require.Equal(t, "true", *lastMeta["deleted"])

	t.Log("PASS: Full lifecycle test completed successfully")
}
