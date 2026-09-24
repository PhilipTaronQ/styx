package common

import (
	"cmp"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/dnr/styx/pb"
)

// fingerprintPrefix starts every signed-message fingerprint, so that a styx signature can't be
// taken for a signature over anything else: a narinfo fingerprint, or the
// "styx-signed-message-1" fingerprint that older styx versions signed, which covered only part
// of the entry.
const fingerprintPrefix = "styx-signed-message-2"

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
// params. The signatures cover every field of both (see messageFingerprint), so everything
// returned is authenticated. A message with fields that this version of styx doesn't know
// can't be verified.
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

	fingerprint, err := messageFingerprint(sm.Msg, sm.Params)
	if err != nil {
		return nil, nil, fmt.Errorf("SignedMessage can't be verified: %w", err)
	} else if !signature.VerifyFirst(fingerprint, sigs, keys) {
		return nil, nil, fmt.Errorf("signature verification failed")
	}

	return sm.Msg, sm.Params, nil
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

func SignMessageAsEntry(keys []signature.SecretKey, params *pb.GlobalParams, e *pb.Entry) ([]byte, error) {
	fingerprint, err := messageFingerprint(e, params)
	if err != nil {
		return nil, err
	}

	sm := &pb.SignedMessage{
		Msg:       e,
		Params:    params,
		KeyId:     make([]string, len(keys)),
		Signature: make([][]byte, len(keys)),
	}
	for i, k := range keys {
		sig, err := k.Sign(rand.Reader, fingerprint)
		if err != nil {
			return nil, err
		}
		sm.KeyId[i] = sig.Name
		sm.Signature[i] = sig.Data
	}

	return proto.Marshal(sm)
}

// messageFingerprint is what a SignedMessage's signatures sign: fingerprintPrefix, then every
// field of the entry (including nested messages) and the params. It doesn't depend on the
// protobuf wire encoding, which isn't canonical: fields are walked by reflection and encoded
// by appendCanonical, so it also covers fields added to the schema later. Unknown fields
// can't be covered, so messages with unknown fields can't be signed or verified.
func messageFingerprint(e *pb.Entry, params *pb.GlobalParams) (string, error) {
	b := make([]byte, 0, 64+len(e.Path)+len(e.InlineData)+len(e.Digests))
	b = append(b, fingerprintPrefix...)
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
