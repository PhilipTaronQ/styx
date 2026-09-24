package common

// Tests written during a bug review. Each one demonstrates a finding and is expected to FAIL
// on the unfixed code.

import (
	"crypto/rand"
	"testing"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dnr/styx/pb"
)

// For a file whose size is an exact multiple of the chunk size, the last chunk is full, but
// AppendBlocksList uses Leftover (0) instead of FileChunkSize and reports 0 blocks for it.
// AllocateBatch then advances the slab sequence by 0, so the next new chunk gets the same
// slab address.
func TestReviewAppendBlocksListExactMultiple(t *testing.T) {
	const blockShift, chunkShift = 12, 16
	assert.Equal(t, []uint16{16}, AppendBlocksList(nil, 1<<16, blockShift, chunkShift), "size 64KiB")
	assert.Equal(t, []uint16{16, 16}, AppendBlocksList(nil, 2<<16, blockShift, chunkShift), "size 128KiB")
	assert.Equal(t, []uint16{256}, AppendBlocksList(nil, 1<<20, blockShift, 20), "size 1MiB, 1MiB chunks")
	// control: not an exact multiple
	assert.Equal(t, []uint16{16, 1}, AppendBlocksList(nil, 1<<16+1, blockShift, chunkShift), "size 64KiB+1")
}

// entryFingerprint covers Path, Size and InlineData/Digests only. ChunkShift, Type,
// ManifestMeta (which the daemon's tarball path reads for chunked manifests) and the
// SignedMessage Params can be changed without invalidating the signature.
func TestReviewSignatureDoesNotCoverUsedFields(t *testing.T) {
	sk, pk, err := signature.GenerateKeypair("styx-review-1", rand.Reader)
	require.NoError(t, err)

	orig := &pb.Entry{
		Path:    ManifestContext + "/00000000000000000000000000000000-review",
		Type:    pb.EntryType_REGULAR,
		Size:    1 << 17,
		Digests: make([]byte, 2*24),
		ManifestMeta: &pb.ManifestMeta{
			GenericTarballResolved: "https://example.org/good.tar.gz",
			Narinfo:                &pb.NarInfo{StorePath: "/nix/store/00000000000000000000000000000000-review"},
		},
	}
	b, err := SignMessageAsEntry([]signature.SecretKey{sk}, &pb.GlobalParams{DigestAlgo: "sha256", DigestBits: 192}, orig)
	require.NoError(t, err)
	_, _, err = VerifyMessageAsEntry([]signature.PublicKey{pk}, ManifestContext, b)
	require.NoError(t, err)

	tamper := func(name string, f func(sm *pb.SignedMessage)) {
		var sm pb.SignedMessage
		require.NoError(t, proto.Unmarshal(b, &sm))
		f(&sm)
		tb, err := proto.Marshal(&sm)
		require.NoError(t, err)
		_, _, err = VerifyMessageAsEntry([]signature.PublicKey{pk}, ManifestContext, tb)
		assert.Error(t, err, "tampered %s still verifies", name)
	}
	tamper("ChunkShift", func(sm *pb.SignedMessage) { sm.Msg.ChunkShift = 30 })
	tamper("ManifestMeta", func(sm *pb.SignedMessage) {
		sm.Msg.ManifestMeta.GenericTarballResolved = "https://evil.example/bad.tar.gz"
		sm.Msg.ManifestMeta.Narinfo.StorePath = "/nix/store/x"
	})
	tamper("Type", func(sm *pb.SignedMessage) { sm.Msg.Type = pb.EntryType_SYMLINK })
	tamper("Params", func(sm *pb.SignedMessage) { sm.Params.DigestBits = 256 })
}
