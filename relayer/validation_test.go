package relayer

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func validSignature() []byte {
	return bytes.Repeat([]byte{0x01}, 65)
}

func TestValidateBasicFields(t *testing.T) {
	valid := &SignedInstantExecute{
		Version:   "1.0",
		Type:      "instant_execute",
		Channel:   "0x1111111111111111111111111111111111111111",
		Nonce:     1,
		Target:    "0x2222222222222222222222222222222222222222",
		Value:     "0",
		Data:      []byte{0x01, 0x02},
		Signature: validSignature(),
		Expires:   9999999999,
		ChainID:   11155111,
	}

	r := &Relayer{}
	if err := r.validateBasicFields(valid); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*SignedInstantExecute)
	}{
		{"bad version", func(m *SignedInstantExecute) { m.Version = "2.0" }},
		{"bad type", func(m *SignedInstantExecute) { m.Type = "channel_create" }},
		{"short signature", func(m *SignedInstantExecute) { m.Signature = []byte{0x01} }},
		{"empty calldata", func(m *SignedInstantExecute) { m.Data = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := *valid
			tt.mutate(&msg)
			if err := r.validateBasicFields(&msg); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestValidateBasicBatchFields(t *testing.T) {
	valid := &SignedBatchInstantExecute{
		Version:   "1.0",
		Type:      "batch_instant_execute",
		Channel:   "0x1111111111111111111111111111111111111111",
		Nonce:     1,
		Signature: validSignature(),
		Expires:   9999999999,
		ChainID:   11155111,
		Items: []BatchItem{
			{
				Target: "0x2222222222222222222222222222222222222222",
				Value:  "0",
				Data:   []byte{0x01},
			},
		},
	}

	r := &Relayer{}
	if err := r.validateBasicBatchFields(valid); err != nil {
		t.Fatalf("valid batch rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*SignedBatchInstantExecute)
	}{
		{"bad version", func(m *SignedBatchInstantExecute) { m.Version = "2.0" }},
		{"bad type", func(m *SignedBatchInstantExecute) { m.Type = "instant_execute" }},
		{"short signature", func(m *SignedBatchInstantExecute) { m.Signature = nil }},
		{"empty items", func(m *SignedBatchInstantExecute) { m.Items = nil }},
		{"empty calldata", func(m *SignedBatchInstantExecute) { m.Items[0].Data = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := *valid
			msg.Items = append([]BatchItem(nil), valid.Items...)
			tt.mutate(&msg)
			if err := r.validateBasicBatchFields(&msg); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestParseWeiAmount(t *testing.T) {
	r := &Relayer{}
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: "0"},
		{in: "0", want: "0"},
		{in: "1000000000000000000", want: "1000000000000000000"},
		{in: "-1", wantErr: true},
		{in: "not-a-number", wantErr: true},
	}
	for _, tt := range tests {
		got, err := r.parseWeiAmount(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("parseWeiAmount(%q) expected error", tt.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseWeiAmount(%q) returned error: %v", tt.in, err)
		}
		if got.String() != tt.want {
			t.Fatalf("parseWeiAmount(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestIsMinimalProxy(t *testing.T) {
	prefix := common.FromHex("0x363d3d373d3d3d363d73")
	suffix := common.FromHex("0x5af43d82803e903d91602b57fd5bf3")
	implementation := common.HexToAddress("0x1234567890123456789012345678901234567890")

	code := make([]byte, 0, len(prefix)+20+len(suffix))
	code = append(code, prefix...)
	code = append(code, implementation.Bytes()...)
	code = append(code, suffix...)

	if !isMinimalProxy(code) {
		t.Fatal("valid EIP-1167 proxy was not detected")
	}
	if isMinimalProxy(nil) {
		t.Fatal("empty code was detected as a proxy")
	}

	malformed := append([]byte(nil), code...)
	malformed[len(malformed)-1] ^= 0xff
	if isMinimalProxy(malformed) {
		t.Fatal("proxy with malformed suffix was detected")
	}
}
