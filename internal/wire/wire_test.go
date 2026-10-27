package wire

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestSchemaSizes(t *testing.T) {
	cases := []struct {
		name string
		sch  Schema
		want int
	}{
		{"account fl", AccountSchema, 816},
		{"symbol Mh", SymbolSchema, 526},
		{"quote Uh", QuoteSchema, 50},
		{"position uu", PositionSchema, 344},
		{"deal xd", DealSchema, 356},
		{"op", OpSchema, 248},
	}
	for _, c := range cases {
		if got := c.sch.Size(); got != c.want {
			t.Errorf("%s size = %d, want %d", c.name, got, c.want)
		}
	}
	if OpSize != OpSchema.Size() {
		t.Errorf("OpSize = %d, schema size = %d", OpSize, OpSchema.Size())
	}
}

func TestFraming(t *testing.T) {
	cmd := BuildCommand(0xAA, 0xBB, CmdLogin, []byte("payload"))
	if cmd[0] != 0xAA || cmd[1] != 0xBB || binary.LittleEndian.Uint16(cmd[2:4]) != CmdLogin || string(cmd[4:]) != "payload" {
		t.Fatalf("command = %x", cmd)
	}
	frame := PackWire([]byte{1, 2, 3})
	if binary.LittleEndian.Uint32(frame[0:4]) != 3 || binary.LittleEndian.Uint32(frame[4:8]) != 1 {
		t.Fatalf("frame header = %x", frame[:8])
	}

	resp := ParseResponse([]byte{0x03, 0x00, 0x1c, 0x00, 0x00, 0x09, 0x08})
	if resp == nil || resp.Tag != 3 || resp.CmdID != 28 || resp.ResCode != 0 || !bytes.Equal(resp.Body, []byte{0x09, 0x08}) {
		t.Fatalf("response = %+v", resp)
	}
	if ParseResponse([]byte{1, 2, 3}) != nil {
		t.Fatal("short payload must parse to nil")
	}
}

func TestParseSeriesQuote(t *testing.T) {
	rec := make([]byte, QuoteSchema.Size())
	binary.LittleEndian.PutUint32(rec[0:4], 42)                         // symbol id
	binary.LittleEndian.PutUint32(rec[4:8], 1700000000)                 // tick time
	binary.LittleEndian.PutUint64(rec[12:20], math.Float64bits(108523)) // raw bid
	binary.LittleEndian.PutUint64(rec[20:28], math.Float64bits(108545)) // raw ask
	vals, off := ParseSeries(rec, QuoteSchema, 0)
	if off != 50 {
		t.Fatalf("offset = %d, want 50", off)
	}
	if vals.Uint(0) != 42 || vals.Int(1) != 1700000000 || vals.Float(3) != 108523 || vals.Float(4) != 108545 {
		t.Fatalf("values = %v", vals)
	}
	// Truncated input yields nils instead of panicking.
	vals, _ = ParseSeries(rec[:10], QuoteSchema, 0)
	if vals[3] != nil {
		t.Fatalf("truncated field = %v, want nil", vals[3])
	}
}

func TestParseSeriesString(t *testing.T) {
	rec := make([]byte, SymbolSchema.Size())
	copy(rec[0:64], EncodeUTF16LE("EURUSD"))
	vals, _ := ParseSeries(rec, SymbolSchema, 0)
	if vals.Str(0) != "EURUSD" {
		t.Fatalf("name = %q", vals.Str(0))
	}
}

func TestPackOpLayout(t *testing.T) {
	op := PackOp(OpParams{
		ActionID: 7, TradeAction: TradeMarket, Symbol: "XAUUSD",
		Volume: 1_000_000, Digits: 2, TradeType: TypeBuy,
		TypeFilling: FillFOK, TypeTime: TimeGTC, TypeFlags: 2,
		Price: 2315.5, SL: 2300, TP: 2400, PriceDeviation: 30,
		Comment: "go", TradePosition: 123456,
	})
	if len(op) != 248 {
		t.Fatalf("len = %d", len(op))
	}
	u32 := func(off int) uint32 { return binary.LittleEndian.Uint32(op[off:]) }
	u64 := func(off int) uint64 { return binary.LittleEndian.Uint64(op[off:]) }
	f64 := func(off int) float64 { return math.Float64frombits(u64(off)) }
	if u32(0) != 7 || u32(4) != TradeMarket || u64(72) != 1_000_000 || u32(80) != 2 ||
		u32(92) != TypeBuy || u32(96) != FillFOK || u32(104) != 2 {
		t.Fatal("header fields misplaced")
	}
	if f64(112) != 2315.5 || f64(128) != 2300 || f64(136) != 2400 || u32(144) != 30 {
		t.Fatal("price fields misplaced")
	}
	if got := DecodeUTF16LE(op[8:72]); got != "XAUUSD" {
		t.Fatalf("symbol = %q", got)
	}
	if got := DecodeUTF16LE(op[164:228]); got != "go" {
		t.Fatalf("comment = %q", got)
	}
	if u64(228) != 123456 {
		t.Fatalf("trade_position = %d", u64(228))
	}
}

func TestParseCandles(t *testing.T) {
	body := make([]byte, 96)
	binary.LittleEndian.PutUint32(body[0:4], 1700000000)
	binary.LittleEndian.PutUint64(body[4:12], math.Float64bits(1.0850))
	binary.LittleEndian.PutUint64(body[12:20], math.Float64bits(1.0860))
	binary.LittleEndian.PutUint64(body[20:28], math.Float64bits(1.0840))
	binary.LittleEndian.PutUint64(body[28:36], math.Float64bits(1.0855))
	binary.LittleEndian.PutUint64(body[36:44], 1234)
	binary.LittleEndian.PutUint32(body[44:48], 11)
	candles := ParseCandles(body)
	if len(candles) != 2 {
		t.Fatalf("count = %d", len(candles))
	}
	c := candles[0]
	if c.Timestamp != 1700000000 || c.Open != 1.0850 || c.High != 1.0860 || c.Low != 1.0840 ||
		c.Close != 1.0855 || c.TickVolume != 1234 || c.Spread != 11 {
		t.Fatalf("candle = %+v", c)
	}
}

func TestVolumeConversion(t *testing.T) {
	if LotsToVolume(0.01) != 1_000_000 || LotsToVolume(1) != 100_000_000 {
		t.Fatal("lots to volume")
	}
	if VolumeToLots(1_000_000) != 0.01 {
		t.Fatal("volume to lots")
	}
	if EstimateSecondsPerCandle(1) != 60 || EstimateSecondsPerCandle(16385) != 3600 ||
		EstimateSecondsPerCandle(32769) != 604800 || EstimateSecondsPerCandle(49153) != 2592000 {
		t.Fatal("timeframe seconds")
	}
}
