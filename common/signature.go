package common

import (
	"cmp"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/dnr/styx/common/cdig"
	"github.com/dnr/styx/pb"
)

// Signed messages carry signatures over one or both of these fingerprints:
//
// v1 covers only the entry's path, size and inline data or digests. It's still produced so
// that older daemons can verify new messages, and accepted so that messages already in
// caches keep working, but a message that verifies only under v1 must have default values
// in every field v1 doesn't cover (see checkV1Entry).
//
// v2 covers every field of the entry and the params (see entryFingerprintV2).
const (
	fingerprintV1 = "styx-signed-message-1"
	fingerprintV2 = "styx-signed-message-2"
)

func LoadPubKeys(keys []string) ([]signature.PublicKey, error) {
	var out []signature.PublicKey
	for _, pk := range keys {
		if k, err := signature.ParsePublicKey(pk); err != nil {
			return nil, err
		} else {
			out = append(out, k)
		}
	}
	return out, nil
}

func LoadSecretKeys(keyfiles []string) ([]signature.SecretKey, error) {
	var out []signature.SecretKey
	for _, path := range keyfiles {
		if skdata, err := os.ReadFile(path); err != nil {
			return nil, err
		} else if k, err := signature.LoadSecretKey(string(skdata)); err != nil {
			return nil, err
		} else {
			out = append(out, k)
		}
	}
	return out, nil
}

// Embedded message must be inline in entry.
func VerifyInlineMessage(
	keys []signature.PublicKey,
	expectedContext string,
	b []byte,
	msg proto.Message,
) error {
	entry, _, err := VerifyMessageAsEntry(keys, expectedContext, b)
	if err != nil {
		return err
	}
	if entry.Size != int64(len(entry.InlineData)) {
		return fmt.Errorf("SignedMessage missing inline data")
	}
	return proto.Unmarshal(entry.InlineData, msg)
}

// VerifyMessageAsEntry checks the signatures on a SignedMessage and returns its entry and
// params. Everything returned is covered by a signature: if the message verifies only under
// the v1 fingerprint, fields that v1 doesn't cover must have their default values, except
// for the ManifestMeta copy that v1 signers put in chunked manifest entries, which is
// removed from the returned entry.
func VerifyMessageAsEntry(keys []signature.PublicKey, expectedContext string, b []byte) (*pb.Entry, *pb.GlobalParams, error) {
	if len(keys) == 0 {
		return nil, nil, fmt.Errorf("no public keys provided")
	}

	var sm pb.SignedMessage
	err := proto.Unmarshal(b, &sm)
	if err != nil {
		return nil, nil, fmt.Errorf("error unmarshaling SignedMessage: %w", err)
	} else if sm.Msg == nil {
		return nil, nil, fmt.Errorf("SignedMessage missing entry")
	} else if sm.Msg.Path != expectedContext && !strings.HasPrefix(sm.Msg.Path, expectedContext+"/") {
		return nil, nil, fmt.Errorf("SignedMessage context mismatch: %q != %q", sm.Msg.Path, expectedContext)
	} else if len(sm.Msg.Digests) > 0 && sm.Params == nil {
		return nil, nil, fmt.Errorf("SignedMessage with chunks must have params")
	}

	sigs := make([]signature.Signature, min(len(sm.KeyId), len(sm.Signature)))
	if len(sigs) == 0 {
		return nil, nil, fmt.Errorf("no signatures in SignedMessage")
	}
	for i := range sigs {
		sigs[i].Name = sm.KeyId[i]
		sigs[i].Data = sm.Signature[i]
	}

	if fp, err := entryFingerprintV2(sm.Msg, sm.Params); err == nil && verifyAny(fp, sigs, keys) {
		return sm.Msg, sm.Params, nil
	}
	if !verifyAny(entryFingerprintV1(sm.Msg), sigs, keys) {
		return nil, nil, fmt.Errorf("signature verification failed")
	}
	if err := checkV1Entry(sm.Msg, sm.Params); err != nil {
		return nil, nil, err
	}
	return sm.Msg, sm.Params, nil
}

// verifyAny returns true if any signature verifies against a key with the same name.
func verifyAny(fingerprint string, sigs []signature.Signature, keys []signature.PublicKey) bool {
	for _, k := range keys {
		for _, sig := range sigs {
			if k.Verify(fingerprint, sig) {
				return true
			}
		}
	}
	return false
}

// checkV1Entry rejects a v1-only message with a non-default value in any field that the v1
// fingerprint doesn't cover: no v1 signer ever set them, so they must have been tampered
// with. The exception is ManifestMeta, which v1 signers copied into chunked manifest entries.
// It can't be authenticated, so it's dropped (after a consistency check).
func checkV1Entry(e *pb.Entry, params *pb.GlobalParams) error {
	if e.Type != pb.EntryType_REGULAR || e.Executable || e.DigestBytes != 0 || e.ChunkShift != 0 ||
		e.StatsInlineData != 0 || e.StatsPresentChunks != 0 || e.StatsPresentBlocks != 0 ||
		len(e.DebugDigests) > 0 || len(e.ProtoReflect().GetUnknown()) > 0 ||
		// v1 covers digests only when there's no inline data
		(len(e.InlineData) > 0 && len(e.Digests) > 0) {
		return errors.New("SignedMessage has fields not covered by its signature")
	}
	if params != nil && (params.DigestAlgo != cdig.Algo || params.DigestBits != cdig.Bits) {
		return errors.New("SignedMessage has params not covered by its signature")
	}
	if mm := e.ManifestMeta; mm != nil {
		if len(e.InlineData) > 0 || path.Base(mm.GetNarinfo().GetStorePath()) != path.Base(e.Path) {
			return errors.New("SignedMessage has inconsistent manifest meta")
		}
		e.ManifestMeta = nil
	}
	return nil
}

func SignInlineMessage(keys []signature.SecretKey, context string, msg proto.Message) ([]byte, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("error marshaling msg: %w", err)
	}
	return SignMessageAsEntry(keys, nil, &pb.Entry{
		Path:       context,
		Type:       pb.EntryType_REGULAR,
		Size:       int64(len(b)),
		InlineData: b,
	})
}

// SignMessageAsEntry signs with both fingerprints. The v1 signatures come first, since older
// daemons check only the first signature for each key.
func SignMessageAsEntry(keys []signature.SecretKey, params *pb.GlobalParams, e *pb.Entry) ([]byte, error) {
	fpV2, err := entryFingerprintV2(e, params)
	if err != nil {
		return nil, err
	}

	sm := &pb.SignedMessage{
		Msg:       e,
		Params:    params,
		KeyId:     make([]string, 0, 2*len(keys)),
		Signature: make([][]byte, 0, 2*len(keys)),
	}
	for _, fingerprint := range []string{entryFingerprintV1(e), fpV2} {
		for _, k := range keys {
			sig, err := k.Sign(rand.Reader, fingerprint)
			if err != nil {
				return nil, err
			}
			sm.KeyId = append(sm.KeyId, sig.Name)
			sm.Signature = append(sm.Signature, sig.Data)
		}
	}

	return proto.Marshal(sm)
}

func entryFingerprintV1(e *pb.Entry) string {
	var sb strings.Builder
	sb.Grow(40 + len(e.Path) + len(e.InlineData) + len(e.Digests))
	sb.WriteString(fingerprintV1)
	sb.WriteByte(0)
	if strings.IndexByte(e.Path, 0) != -1 {
		panic("nil in entry path")
	}
	sb.WriteString(e.Path)
	sb.WriteByte(0)
	sb.WriteString(strconv.Itoa(int(e.Size)))
	if len(e.InlineData) > 0 {
		sb.WriteByte(1)
		sb.Write(e.InlineData)
	} else {
		sb.WriteByte(2)
		sb.Write(e.Digests)
	}
	return sb.String()
}

// entryFingerprintV2 covers every field of the entry (including nested messages) and the
// params. It doesn't depend on the protobuf wire encoding, which isn't canonical: fields are
// walked by reflection and encoded with appendCanonical. Unknown fields can't be covered, so
// messages with unknown fields can't be signed or verified under v2.
func entryFingerprintV2(e *pb.Entry, params *pb.GlobalParams) (string, error) {
	b := make([]byte, 0, 64+len(e.Path)+len(e.InlineData)+len(e.Digests))
	b = append(b, fingerprintV2...)
	b = append(b, 0)
	b, err := appendCanonical(b, e.ProtoReflect())
	if err != nil {
		return "", err
	}
	if params == nil {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		if b, err = appendCanonical(b, params.ProtoReflect()); err != nil {
			return "", err
		}
	}
	return string(b), nil
}

// appendCanonical appends an unambiguous encoding of m: the number of populated fields, then
// each populated field's number and value, in field number order.
func appendCanonical(b []byte, m protoreflect.Message) ([]byte, error) {
	if len(m.GetUnknown()) > 0 {
		return nil, fmt.Errorf("can't sign %s with unknown fields", m.Descriptor().FullName())
	}
	var fds []protoreflect.FieldDescriptor
	m.Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		fds = append(fds, fd)
		return true
	})
	slices.SortFunc(fds, func(x, y protoreflect.FieldDescriptor) int {
		return cmp.Compare(x.Number(), y.Number())
	})

	b = binary.AppendUvarint(b, uint64(len(fds)))
	var err error
	for _, fd := range fds {
		b = binary.AppendUvarint(b, uint64(fd.Number()))
		v := m.Get(fd)
		switch {
		case fd.IsMap():
			return nil, fmt.Errorf("can't sign map field %s", fd.FullName())
		case fd.IsList():
			l := v.List()
			b = binary.AppendUvarint(b, uint64(l.Len()))
			for i := range l.Len() {
				if b, err = appendCanonicalValue(b, fd, l.Get(i)); err != nil {
					return nil, err
				}
			}
		default:
			if b, err = appendCanonicalValue(b, fd, v); err != nil {
				return nil, err
			}
		}
	}
	return b, nil
}

func appendCanonicalValue(b []byte, fd protoreflect.FieldDescriptor, v protoreflect.Value) ([]byte, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		if v.Bool() {
			return append(b, 1), nil
		}
		return append(b, 0), nil
	case protoreflect.EnumKind:
		return binary.AppendVarint(b, int64(v.Enum())), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return binary.AppendVarint(b, v.Int()), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return binary.AppendUvarint(b, v.Uint()), nil
	case protoreflect.StringKind:
		b = binary.AppendUvarint(b, uint64(len(v.String())))
		return append(b, v.String()...), nil
	case protoreflect.BytesKind:
		b = binary.AppendUvarint(b, uint64(len(v.Bytes())))
		return append(b, v.Bytes()...), nil
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return appendCanonical(b, v.Message())
	default:
		return nil, fmt.Errorf("can't sign %s field %s", fd.Kind(), fd.FullName())
	}
}
