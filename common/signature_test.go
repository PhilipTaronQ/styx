package common

import (
	"crypto/rand"
	"fmt"
	"slices"
	"testing"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/dnr/styx/pb"
)

// The v1 fingerprint covers only Path, Size and InlineData/Digests. ChunkShift, Type,
// ManifestMeta (which the daemon's tarball path reads for chunked manifests) and the
// SignedMessage Params could be changed without invalidating the signature.
func TestSignatureDoesNotCoverUsedFields(t *testing.T) {
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
	tamper("ManifestMeta", func(sm *pb.SignedMessage) {
		sm.Msg.ManifestMeta.GenericTarballResolved = "https://evil.example/bad.tar.gz"
		sm.Msg.ManifestMeta.Narinfo.StorePath = "/nix/store/x"
	})
	tamper("Type", func(sm *pb.SignedMessage) { sm.Msg.Type = pb.EntryType_SYMLINK })
	tamper("Params", func(sm *pb.SignedMessage) { sm.Params.DigestBits = 256 })
}

// The daemon uses the envelope's chunk_shift to allocate and slice chunked manifests, and
// handleTarballReq takes the narinfo and resolved upstream of a chunked tarball manifest
// from manifest_meta. Anyone who can write the manifest cache could change them without
// breaking the signature. A tampered value must either fail verification or not be
// returned.
func TestSignatureCoversAllUsedEntryFields(t *testing.T) {
	sk, pk, err := signature.GenerateKeypair("test-1", rand.Reader)
	require.NoError(t, err)

	entry := &pb.Entry{
		Path:    ManifestContext + "/53qwclnym7a6vzs937jjmsfqxlxlsf2y-opusfile-0.12",
		Type:    pb.EntryType_REGULAR,
		Size:    100000,
		Digests: make([]byte, 2*24),
		ManifestMeta: &pb.ManifestMeta{
			GenericTarballResolved: "https://example.org/good.tar.gz",
		},
	}
	signed, err := SignMessageAsEntry([]signature.SecretKey{sk}, &pb.GlobalParams{DigestAlgo: "sha256", DigestBits: 192}, entry)
	require.NoError(t, err)
	_, _, err = VerifyMessageAsEntry([]signature.PublicKey{pk}, ManifestContext, signed)
	require.NoError(t, err, "precondition: untampered envelope verifies")

	for name, tamper := range map[string]func(*pb.Entry){
		"chunk_shift": func(e *pb.Entry) { e.ChunkShift = -1 },
		"manifest_meta": func(e *pb.Entry) {
			e.ManifestMeta.GenericTarballResolved = "https://attacker.example/evil.tar.gz"
		},
	} {
		var sm pb.SignedMessage
		require.NoError(t, proto.Unmarshal(signed, &sm))
		tamper(sm.Msg)
		tampered, err := proto.Marshal(&sm)
		require.NoError(t, err)
		got, _, err := VerifyMessageAsEntry([]signature.PublicKey{pk}, ManifestContext, tampered)
		if err == nil {
			assert.NotEqual(t, "https://attacker.example/evil.tar.gz", got.ManifestMeta.GetGenericTarballResolved())
			assert.Zero(t, got.ChunkShift, "envelope with tampered %s still verifies", name)
		}
	}
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
			Path:    ManifestContext + "/" + sp,
			Type:    pb.EntryType_REGULAR,
			Size:    100000,
			Digests: slices.Repeat([]byte{7}, 2*24),
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

// signV1Only signs like styx did before the v2 fingerprint existed.
func signV1Only(t *testing.T, sk signature.SecretKey, params *pb.GlobalParams, e *pb.Entry) []byte {
	sig, err := sk.Sign(rand.Reader, entryFingerprintV1(e))
	require.NoError(t, err)
	b, err := proto.Marshal(&pb.SignedMessage{
		Msg:       e,
		Params:    params,
		KeyId:     []string{sig.Name},
		Signature: [][]byte{sig.Data},
	})
	require.NoError(t, err)
	return b
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

// Property: changing any field of the entry or params (at any depth, including fields added
// to the schema later) either breaks verification or doesn't change what verification
// returns. The only thing verification may drop is the ManifestMeta of a v1-only message.
// This holds both for messages signed now and for messages signed before v2 existed.
func TestSignatureTamperedFieldsDontVerify(t *testing.T) {
	sk, pk, err := signature.GenerateKeypair("test-1", rand.Reader)
	require.NoError(t, err)
	pks := []signature.PublicKey{pk}

	signers := map[string]func(*pb.GlobalParams, *pb.Entry) []byte{
		"current": func(p *pb.GlobalParams, e *pb.Entry) []byte {
			b, err := SignMessageAsEntry([]signature.SecretKey{sk}, p, e)
			require.NoError(t, err)
			return b
		},
		"v1 only": func(p *pb.GlobalParams, e *pb.Entry) []byte { return signV1Only(t, sk, p, e) },
	}

	for _, tc := range sigTestCases() {
		for signerName, sign := range signers {
			t.Run(tc.name+"/"+signerName, func(t *testing.T) {
				signed := sign(tc.params, tc.entry)
				gotE, gotP, err := VerifyMessageAsEntry(pks, tc.context, signed)
				require.NoError(t, err, "untampered message must verify")
				want := proto.Clone(tc.entry).(*pb.Entry)
				if signerName == "v1 only" {
					want.ManifestMeta = nil
				}
				require.True(t, proto.Equal(want, gotE), "got %v", gotE)
				require.True(t, proto.Equal(tc.params, gotP), "got %v", gotP)

				check := func(name string, sm *pb.SignedMessage) {
					b, err := proto.Marshal(sm)
					require.NoError(t, err)
					gotE, gotP, err := VerifyMessageAsEntry(pks, tc.context, b)
					if err != nil {
						return
					}
					want := proto.Clone(tc.entry).(*pb.Entry)
					if gotE.ManifestMeta == nil {
						want.ManifestMeta = nil
					}
					assert.True(t, proto.Equal(want, gotE) && proto.Equal(tc.params, gotP),
						"tampered %s verified: got %v %v", name, gotE, gotP)
				}
				mutateEachField(tc.entry, func(name string, mutated proto.Message) {
					var sm pb.SignedMessage
					require.NoError(t, proto.Unmarshal(signed, &sm))
					sm.Msg = mutated.(*pb.Entry)
					check("entry."+name, &sm)
				})
				if tc.params != nil {
					mutateEachField(tc.params, func(name string, mutated proto.Message) {
						var sm pb.SignedMessage
						require.NoError(t, proto.Unmarshal(signed, &sm))
						sm.Params = mutated.(*pb.GlobalParams)
						check("params."+name, &sm)
					})
				}

				// fields that this version doesn't know about can't be covered either
				var sm pb.SignedMessage
				require.NoError(t, proto.Unmarshal(signed, &sm))
				sm.Msg.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
				check("entry unknown field", &sm)
			})
		}
	}
}

// Daemons that only know the v1 fingerprint check the first signature for each key (see
// signature.VerifyFirst), so new messages must still verify for them.
func TestSignatureVerifiesForOldDaemons(t *testing.T) {
	sk1, pk1, err := signature.GenerateKeypair("test-1", rand.Reader)
	require.NoError(t, err)
	sk2, pk2, err := signature.GenerateKeypair("test-2", rand.Reader)
	require.NoError(t, err)

	for _, tc := range sigTestCases() {
		b, err := SignMessageAsEntry([]signature.SecretKey{sk1, sk2}, tc.params, tc.entry)
		require.NoError(t, err)
		var sm pb.SignedMessage
		require.NoError(t, proto.Unmarshal(b, &sm))
		sigs := make([]signature.Signature, len(sm.KeyId))
		for i := range sigs {
			sigs[i] = signature.Signature{Name: sm.KeyId[i], Data: sm.Signature[i]}
		}
		for _, pk := range []signature.PublicKey{pk1, pk2} {
			assert.True(t, signature.VerifyFirst(entryFingerprintV1(sm.Msg), sigs, []signature.PublicKey{pk}), tc.name)
			_, _, err := VerifyMessageAsEntry([]signature.PublicKey{pk}, tc.context, b)
			assert.NoError(t, err, tc.name)
		}
	}
}

// Manifests signed before the variable chunk size change carry chunk_shift = 16 in params
// field 1, now reserved. They must still verify.
func TestSignatureV1ParamsWithReservedField(t *testing.T) {
	sk, pk, err := signature.GenerateKeypair("test-1", rand.Reader)
	require.NoError(t, err)

	var params pb.GlobalParams
	pb1, err := proto.Marshal(&pb.GlobalParams{DigestAlgo: "sha256", DigestBits: 192})
	require.NoError(t, err)
	pb1 = protowire.AppendVarint(protowire.AppendTag(pb1, 1, protowire.VarintType), 16)
	require.NoError(t, proto.Unmarshal(pb1, &params))
	require.NotEmpty(t, params.ProtoReflect().GetUnknown())

	tc := sigTestCases()[0]
	b := signV1Only(t, sk, &params, tc.entry)
	e, _, err := VerifyMessageAsEntry([]signature.PublicKey{pk}, tc.context, b)
	require.NoError(t, err)
	assert.Nil(t, e.ManifestMeta, "v1 manifest meta can't be authenticated")
}
