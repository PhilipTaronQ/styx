package common

import (
	"crypto/rand"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/dnr/styx/pb"
)

// The daemon uses the envelope's chunk_shift to allocate and slice chunked manifests (a bad
// value panics it), and handleTarballReq takes the narinfo and resolved upstream of a chunked
// tarball manifest from manifest_meta. Changing any of them, the type or the params must break
// the signature.
func TestSignatureCoversUsedFields(t *testing.T) {
	sk, pk, err := signature.GenerateKeypair("styx-test-1", rand.Reader)
	require.NoError(t, err)

	orig := &pb.Entry{
		Path:    ManifestContext + "/00000000000000000000000000000000-test",
		Type:    pb.EntryType_REGULAR,
		Size:    1 << 17,
		Digests: make([]byte, 2*24),
		ManifestMeta: &pb.ManifestMeta{
			GenericTarballResolved: "https://example.org/good.tar.gz",
			Narinfo:                &pb.NarInfo{StorePath: "/nix/store/00000000000000000000000000000000-test"},
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
	tamper("ChunkShift negative", func(sm *pb.SignedMessage) { sm.Msg.ChunkShift = -1 })
	tamper("ManifestMeta", func(sm *pb.SignedMessage) {
		sm.Msg.ManifestMeta.GenericTarballResolved = "https://evil.example/bad.tar.gz"
	})
	tamper("ManifestMeta narinfo", func(sm *pb.SignedMessage) { sm.Msg.ManifestMeta.Narinfo.StorePath = "/nix/store/x" })
	tamper("ManifestMeta removed", func(sm *pb.SignedMessage) { sm.Msg.ManifestMeta = nil })
	tamper("Type", func(sm *pb.SignedMessage) { sm.Msg.Type = pb.EntryType_SYMLINK })
	tamper("Params", func(sm *pb.SignedMessage) { sm.Params.DigestBits = 256 })
}

type sigTestCase struct {
	name    string
	context string
	params  *pb.GlobalParams
	entry   *pb.Entry
}

func sigTestCases() []sigTestCase {
	sp := "53qwclnym7a6vzs937jjmsfqxlxlsf2y-opusfile-0.12"
	return []sigTestCase{{
		name:    "chunked manifest",
		context: ManifestContext,
		params:  &pb.GlobalParams{DigestAlgo: "sha256", DigestBits: 192},
		entry: &pb.Entry{
			Path:       ManifestContext + "/" + sp,
			Type:       pb.EntryType_REGULAR,
			Size:       100000,
			Digests:    slices.Repeat([]byte{7}, 2*24),
			ChunkShift: 17,
			ManifestMeta: &pb.ManifestMeta{
				NarinfoUrl: "https://cache.example/" + sp[:32] + ".narinfo",
				Narinfo: &pb.NarInfo{
					StorePath:  "/nix/store/" + sp,
					NarHash:    "sha256:1111111111111111111111111111111111111111111111111111",
					NarSize:    100000,
					References: []string{sp},
				},
				GenericTarballResolved: "https://example.org/good.tar.gz",
				Generator:              "styx-test",
				GeneratedTime:          1700000000,
			},
		},
	}, {
		name:    "inline manifest",
		context: ManifestContext,
		params:  &pb.GlobalParams{DigestAlgo: "sha256", DigestBits: 192},
		entry: &pb.Entry{
			Path:       ManifestContext + "/" + sp,
			Type:       pb.EntryType_REGULAR,
			Size:       5,
			InlineData: []byte("hello"),
		},
	}, {
		name:    "inline message without params",
		context: DaemonParamsContext,
		entry: &pb.Entry{
			Path:       DaemonParamsContext,
			Type:       pb.EntryType_REGULAR,
			Size:       5,
			InlineData: []byte("hello"),
		},
	}}
}

// mutateEachField calls f once for every scalar or repeated field in msg's schema, recursing
// into message fields, with a copy of msg in which that field has a different value.
func mutateEachField(msg proto.Message, f func(name string, mutated proto.Message)) {
	var walk func(prefix string, parents []protoreflect.FieldDescriptor, md protoreflect.MessageDescriptor)
	walk = func(prefix string, parents []protoreflect.FieldDescriptor, md protoreflect.MessageDescriptor) {
		fields := md.Fields()
		for i := range fields.Len() {
			fd := fields.Get(i)
			name := prefix + string(fd.Name())
			if fd.Kind() == protoreflect.MessageKind && !fd.IsList() {
				walk(name+".", append(slices.Clone(parents), fd), fd.Message())
				continue
			}
			c := proto.Clone(msg)
			m := c.ProtoReflect()
			for _, p := range parents {
				m = m.Mutable(p).Message()
			}
			if fd.IsList() {
				m.Mutable(fd).List().Append(otherValue(fd, zeroValue(fd)))
			} else {
				m.Set(fd, otherValue(fd, m.Get(fd)))
			}
			f(name, c)
		}
	}
	walk("", nil, msg.ProtoReflect().Descriptor())
}

func zeroValue(fd protoreflect.FieldDescriptor) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(false)
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(0)
	case protoreflect.Int32Kind:
		return protoreflect.ValueOfInt32(0)
	case protoreflect.Int64Kind:
		return protoreflect.ValueOfInt64(0)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(nil)
	}
	panic(fmt.Sprintf("add a zero value for %s fields", fd.Kind()))
}

func otherValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(!v.Bool())
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(v.Enum() + 1)
	case protoreflect.Int32Kind:
		return protoreflect.ValueOfInt32(int32(v.Int()) + 1)
	case protoreflect.Int64Kind:
		return protoreflect.ValueOfInt64(v.Int() + 1)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(v.String() + "x")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(append(slices.Clone(v.Bytes()), 0xff))
	}
	panic(fmt.Sprintf("add a mutation for %s fields", fd.Kind()))
}

// Property: changing any field of the entry or params, at any depth (including fields added
// to the schema later), breaks verification. So does a field this version doesn't know.
func TestSignatureTamperedFieldsDontVerify(t *testing.T) {
	sk, pk, err := signature.GenerateKeypair("test-1", rand.Reader)
	require.NoError(t, err)
	pks := []signature.PublicKey{pk}

	for _, tc := range sigTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			signed, err := SignMessageAsEntry([]signature.SecretKey{sk}, tc.params, tc.entry)
			require.NoError(t, err)
			gotE, gotP, err := VerifyMessageAsEntry(pks, tc.context, signed)
			require.NoError(t, err, "untampered message must verify")
			require.True(t, proto.Equal(tc.entry, gotE), "got %v", gotE)
			require.True(t, proto.Equal(tc.params, gotP), "got %v", gotP)

			check := func(name string, f func(sm *pb.SignedMessage)) {
				var sm pb.SignedMessage
				require.NoError(t, proto.Unmarshal(signed, &sm))
				f(&sm)
				b, err := proto.Marshal(&sm)
				require.NoError(t, err)
				gotE, gotP, err := VerifyMessageAsEntry(pks, tc.context, b)
				assert.Error(t, err, "tampered %s verified: got %v %v", name, gotE, gotP)
			}
			var n int
			mutateEachField(tc.entry, func(name string, mutated proto.Message) {
				n++
				check("entry."+name, func(sm *pb.SignedMessage) { sm.Msg = mutated.(*pb.Entry) })
			})
			require.Greater(t, n, 20, "didn't walk the entry's fields")
			if tc.params != nil {
				mutateEachField(tc.params, func(name string, mutated proto.Message) {
					check("params."+name, func(sm *pb.SignedMessage) { sm.Params = mutated.(*pb.GlobalParams) })
				})
				check("params removed", func(sm *pb.SignedMessage) { sm.Params = nil })
			} else {
				check("params added", func(sm *pb.SignedMessage) {
					sm.Params = &pb.GlobalParams{DigestAlgo: "sha256", DigestBits: 192}
				})
			}

			unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)
			check("entry unknown field", func(sm *pb.SignedMessage) { sm.Msg.ProtoReflect().SetUnknown(unknown) })
			check("params unknown field", func(sm *pb.SignedMessage) {
				if sm.Params == nil {
					sm.Params = &pb.GlobalParams{}
				}
				sm.Params.ProtoReflect().SetUnknown(unknown)
			})
			if tc.entry.ManifestMeta != nil {
				check("manifest meta unknown field", func(sm *pb.SignedMessage) {
					sm.Msg.ManifestMeta.Narinfo.ProtoReflect().SetUnknown(unknown)
				})
			}
		})
	}
}

// Every key signs, and any one of them verifies.
func TestSignatureMultipleKeys(t *testing.T) {
	sk1, pk1, err := signature.GenerateKeypair("test-1", rand.Reader)
	require.NoError(t, err)
	sk2, pk2, err := signature.GenerateKeypair("test-2", rand.Reader)
	require.NoError(t, err)
	_, pk3, err := signature.GenerateKeypair("test-3", rand.Reader)
	require.NoError(t, err)

	tc := sigTestCases()[0]
	b, err := SignMessageAsEntry([]signature.SecretKey{sk1, sk2}, tc.params, tc.entry)
	require.NoError(t, err)
	for _, pk := range []signature.PublicKey{pk1, pk2} {
		_, _, err := VerifyMessageAsEntry([]signature.PublicKey{pk}, tc.context, b)
		assert.NoError(t, err, pk.Name)
	}
	_, _, err = VerifyMessageAsEntry([]signature.PublicKey{pk3}, tc.context, b)
	assert.Error(t, err, "verified without a matching key")
}

// Signatures over the fingerprint older styx versions used (path, size and data or digests
// only) aren't accepted: there's no fallback.
func TestSignatureRejectsOldFingerprint(t *testing.T) {
	sk, pk, err := signature.GenerateKeypair("test-1", rand.Reader)
	require.NoError(t, err)

	for _, tc := range sigTestCases() {
		e := tc.entry
		fp := "styx-signed-message-1\x00" + e.Path + "\x00" + strconv.Itoa(int(e.Size))
		if len(e.InlineData) > 0 {
			fp += "\x01" + string(e.InlineData)
		} else {
			fp += "\x02" + string(e.Digests)
		}
		sig, err := sk.Sign(rand.Reader, fp)
		require.NoError(t, err)
		b, err := proto.Marshal(&pb.SignedMessage{
			Msg:       e,
			Params:    tc.params,
			KeyId:     []string{sig.Name},
			Signature: [][]byte{sig.Data},
		})
		require.NoError(t, err)
		_, _, err = VerifyMessageAsEntry([]signature.PublicKey{pk}, tc.context, b)
		assert.Error(t, err, tc.name)
	}
}
