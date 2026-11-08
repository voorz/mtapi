package mtapi

import (
	"encoding/binary"
	"testing"

	"github.com/voorz/mtapi/internal/wire"
)

// fieldOffset returns the byte offset of field idx inside one record
// of the schema, walking the same size rules as wire.Schema.Size.
func fieldOffset(s wire.Schema, idx int) int {
	off := 0
	for i, f := range s {
		if i == idx {
			return off
		}
		switch f.Type {
		case wire.TString, wire.TBytes:
			off += f.Length
		case wire.TInt8, wire.TUint8:
			off++
		case wire.TInt16, wire.TUint16:
			off += 2
		case wire.TInt32, wire.TUint32, wire.TFloat32:
			off += 4
		default: // int64, uint64, float64
			off += 8
		}
	}
	return -1
}

func putU32(rec []byte, s wire.Schema, idx int, v uint32) {
	binary.LittleEndian.PutUint32(rec[fieldOffset(s, idx):], v)
}

func putI64(rec []byte, s wire.Schema, idx int, v int64) {
	binary.LittleEndian.PutUint64(rec[fieldOffset(s, idx):], uint64(v))
}

// TestParseDealsVerifiedFields pins the deal fields verified on a live
// account on 2026-11-09: entry (field 6), position id (field 19) and
// the origin marker / magic (field 24). A regression in the schema
// layout must fail here, not on a real account.
func TestParseDealsVerifiedFields(t *testing.T) {
	size := wire.DealSchema.Size()
	body := make([]byte, 4+2*size)
	binary.LittleEndian.PutUint32(body[0:4], 2)

	ea := body[4 : 4+size]
	putI64(ea, wire.DealSchema, 0, 9001)    // ticket
	putU32(ea, wire.DealSchema, 6, 1)       // entry: out (close)
	putI64(ea, wire.DealSchema, 19, 777001) // position id
	putU32(ea, wire.DealSchema, 24, 424242) // magic: EA

	manual := body[4+size : 4+2*size]
	putI64(manual, wire.DealSchema, 0, 9002)
	putU32(manual, wire.DealSchema, 6, 0) // entry: in (open)
	putI64(manual, wire.DealSchema, 19, 777002)
	// magic left 0: manual trading

	deals := parseDeals(body)
	if len(deals) != 2 {
		t.Fatalf("parseDeals: got %d deals, want 2", len(deals))
	}
	if deals[0].Entry != DealEntryOut {
		t.Errorf("deal 0 entry = %d, want DealEntryOut (%d)", deals[0].Entry, DealEntryOut)
	}
	if deals[0].Position != 777001 {
		t.Errorf("deal 0 position = %d, want 777001", deals[0].Position)
	}
	if deals[0].Magic != 424242 {
		t.Errorf("deal 0 magic = %d, want 424242", deals[0].Magic)
	}
	if deals[1].Entry != DealEntryIn {
		t.Errorf("deal 1 entry = %d, want DealEntryIn (%d)", deals[1].Entry, DealEntryIn)
	}
	if deals[1].Position != 777002 {
		t.Errorf("deal 1 position = %d, want 777002", deals[1].Position)
	}
	if deals[1].Magic != 0 {
		t.Errorf("deal 1 magic = %d, want 0 (manual)", deals[1].Magic)
	}
}

// TestParsePositionsMagic pins the position origin marker (field 22),
// verified on a live account on 2026-11-09.
func TestParsePositionsMagic(t *testing.T) {
	size := wire.PositionSchema.Size()
	body := make([]byte, 4+2*size+4)
	binary.LittleEndian.PutUint32(body[0:4], 2)

	ea := body[4 : 4+size]
	putI64(ea, wire.PositionSchema, 0, 5001)
	putU32(ea, wire.PositionSchema, 22, 99) // magic: EA

	manual := body[4+size : 4+2*size]
	putI64(manual, wire.PositionSchema, 0, 5002)
	// magic left 0: manual trading

	positions := parsePositions(body)
	if len(positions) != 2 {
		t.Fatalf("parsePositions: got %d positions, want 2", len(positions))
	}
	if positions[0].Magic != 99 {
		t.Errorf("position 0 magic = %d, want 99", positions[0].Magic)
	}
	if positions[1].Magic != 0 {
		t.Errorf("position 1 magic = %d, want 0 (manual)", positions[1].Magic)
	}
}
