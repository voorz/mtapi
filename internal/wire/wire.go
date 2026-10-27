// Package wire implements the MT5 WebSocket protocol's binary framing:
// the outer wire packet, the command envelope inside the encrypted
// payload, the response envelope, and the schema-driven binary
// serialization system (Ac.parse / Ac.serialize in the terminal's
// JavaScript) used by account, symbol, quote, position, deal and trade
// payloads.
//
// Everything here mirrors the reference Python module
// (MT5API/mt5client/protocol.py) byte for byte.
package wire

import (
	"encoding/binary"
	"math"
	"unicode/utf16"
)

// ---------------------------------------------------------------------------
// Command IDs and protocol constants
// ---------------------------------------------------------------------------

// Command IDs on the wire.
const (
	CmdAuth       = 0
	CmdLogout     = 2
	CmdAccount    = 3
	CmdPositions  = 4
	CmdDeals      = 5
	CmdSubscribe  = 7
	CmdQuotes     = 8
	CmdRates      = 11
	CmdTrade      = 12
	CmdAcctUpdate = 14
	CmdSystem     = 15
	CmdSymbolSpec = 17
	CmdTradeEvent = 19
	CmdPosUpdate  = 22
	CmdLogin      = 28
	CmdSymbols    = 34
	CmdNotify     = 42
	CmdHeartbeat  = 51
)

// Trade actions (the Op struct's trade_action field).
const (
	TradeRequest     = 1
	TradeInstant     = 2
	TradeMarket      = 3
	TradeExchange    = 4
	TradePending     = 5
	TradeModify      = 6 // modify a position's SL/TP
	TradeModifyOrder = 7 // modify a pending order
	TradeCancel      = 8
)

// Order/position types (the Op struct's trade_type field, and the type
// fields of positions, orders and deals).
const (
	TypeBuy           = 0
	TypeSell          = 1
	TypeBuyLimit      = 2
	TypeSellLimit     = 3
	TypeBuyStop       = 4
	TypeSellStop      = 5
	TypeBuyStopLimit  = 6
	TypeSellStopLimit = 7
)

// Filling modes.
const (
	FillFOK    = 0
	FillIOC    = 1
	FillReturn = 2
)

// Order lifetime types.
const (
	TimeGTC       = 0
	TimeDay       = 1
	TimeSpecified = 2
)

// Trade modes reported in symbol specs.
const (
	TradeDisabled  = 0
	TradeLongOnly  = 1
	TradeShortOnly = 2
	TradeCloseOnly = 3
	TradeFull      = 4
)

// LotMultiplier converts between lots at the API surface and the raw
// integer volume used on the wire: raw = lots * LotMultiplier.
const LotMultiplier = 100_000_000

// LotsToVolume converts lots to the raw wire volume.
func LotsToVolume(lots float64) uint64 {
	if lots <= 0 {
		return 0
	}
	return uint64(lots * LotMultiplier)
}

// VolumeToLots converts a raw wire volume back to lots.
func VolumeToLots(raw uint64) float64 {
	return float64(raw) / LotMultiplier
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

// PackWire wraps an encrypted payload in the outer wire frame:
// [payload_length:4 LE][version=1:4 LE][encrypted payload].
func PackWire(encrypted []byte) []byte {
	frame := make([]byte, 8+len(encrypted))
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(encrypted)))
	binary.LittleEndian.PutUint32(frame[4:8], 1)
	copy(frame[8:], encrypted)
	return frame
}

// BuildCommand builds the plaintext command envelope:
// [random:2][cmd_id:2 LE][payload]. The two random bytes come from the
// caller so tests can pin them.
func BuildCommand(rand1, rand2 byte, cmdID uint16, payload []byte) []byte {
	cmd := make([]byte, 4+len(payload))
	cmd[0] = rand1
	cmd[1] = rand2
	binary.LittleEndian.PutUint16(cmd[2:4], cmdID)
	copy(cmd[4:], payload)
	return cmd
}

// Response is a parsed response envelope:
// [tag:2 LE][cmd_id:2 LE][res_code:1][body:N].
type Response struct {
	Tag     uint16
	CmdID   uint16
	ResCode byte
	Body    []byte
}

// ParseResponse parses a decrypted payload into a Response. It returns
// nil when the payload is too short to hold an envelope, mirroring the
// reference.
func ParseResponse(data []byte) *Response {
	if len(data) < 5 {
		return nil
	}
	return &Response{
		Tag:     binary.LittleEndian.Uint16(data[0:2]),
		CmdID:   binary.LittleEndian.Uint16(data[2:4]),
		ResCode: data[4],
		Body:    data[5:],
	}
}

// ---------------------------------------------------------------------------
// Schema-driven binary serialization (Ac.parse)
// ---------------------------------------------------------------------------

// Field types of the serialization system. Note that type 6 is a
// 4-byte uint32, not a uint64.
const (
	TInt8    = 1
	TInt16   = 2
	TInt32   = 3
	TUint8   = 4
	TUint16  = 5
	TUint32  = 6
	TFloat32 = 7
	TFloat64 = 8
	TString  = 11 // UTF-16LE, Length bytes, NUL-terminated
	TBytes   = 12 // raw, Length bytes
	TInt64   = 17
	TUint64  = 18
)

var typeSizes = map[int]int{
	TInt8: 1, TInt16: 2, TInt32: 4, TUint8: 1, TUint16: 2, TUint32: 4,
	TFloat32: 4, TFloat64: 8, TInt64: 8, TUint64: 8,
}

// Field is one entry of a serialization schema. Length is the byte
// length for TString and TBytes fields and is ignored otherwise.
type Field struct {
	Type   int
	Length int
}

// Schema is an ordered list of fields describing one binary record.
type Schema []Field

// Size returns the total byte size of one record of the schema.
func (s Schema) Size() int {
	size := 0
	for _, f := range s {
		if sz, ok := typeSizes[f.Type]; ok {
			size += sz
		} else if f.Type == TString || f.Type == TBytes {
			size += f.Length
		}
	}
	return size
}

// Values holds one parsed record. Entries are typed as int8, int16,
// int32, uint8, uint16, uint32, float32, float64, int64, uint64, string
// or []byte; a field that falls outside the input data is nil, exactly
// as in the reference parser.
type Values []any

// ParseSeries parses one record of the schema from data starting at
// offset, returning the values and the offset just past the record.
func ParseSeries(data []byte, schema Schema, offset int) (Values, int) {
	vals := make(Values, len(schema))
	for i, f := range schema {
		if offset >= len(data) {
			vals[i] = nil
			continue
		}
		if sz, ok := typeSizes[f.Type]; ok {
			if offset+sz > len(data) {
				vals[i] = nil
				offset = len(data)
				continue
			}
			b := data[offset : offset+sz]
			switch f.Type {
			case TInt8:
				vals[i] = int8(b[0])
			case TInt16:
				vals[i] = int16(binary.LittleEndian.Uint16(b))
			case TInt32:
				vals[i] = int32(binary.LittleEndian.Uint32(b))
			case TUint8:
				vals[i] = b[0]
			case TUint16:
				vals[i] = binary.LittleEndian.Uint16(b)
			case TUint32:
				vals[i] = binary.LittleEndian.Uint32(b)
			case TFloat32:
				vals[i] = math.Float32frombits(binary.LittleEndian.Uint32(b))
			case TFloat64:
				vals[i] = math.Float64frombits(binary.LittleEndian.Uint64(b))
			case TInt64:
				vals[i] = int64(binary.LittleEndian.Uint64(b))
			case TUint64:
				vals[i] = binary.LittleEndian.Uint64(b)
			}
			offset += sz
			continue
		}
		switch f.Type {
		case TString:
			if offset+f.Length > len(data) {
				vals[i] = nil
				offset = len(data)
				continue
			}
			vals[i] = DecodeUTF16LE(data[offset : offset+f.Length])
			offset += f.Length
		case TBytes:
			if offset+f.Length > len(data) {
				vals[i] = nil
				offset = len(data)
				continue
			}
			raw := make([]byte, f.Length)
			copy(raw, data[offset:offset+f.Length])
			vals[i] = raw
			offset += f.Length
		default:
			vals[i] = nil
		}
	}
	return vals, offset
}

// Int returns the field at i as int64, or 0 if absent or non-numeric.
func (v Values) Int(i int) int64 {
	if i < 0 || i >= len(v) || v[i] == nil {
		return 0
	}
	switch n := v[i].(type) {
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case int64:
		return n
	case uint64:
		return int64(n)
	case float32:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

// Uint returns the field at i as uint64, or 0 if absent or non-numeric.
func (v Values) Uint(i int) uint64 {
	if i < 0 || i >= len(v) || v[i] == nil {
		return 0
	}
	switch n := v[i].(type) {
	case uint64:
		return n
	case int64:
		return uint64(n)
	case uint32:
		return uint64(n)
	case int32:
		return uint64(n)
	case uint16:
		return uint64(n)
	case int16:
		return uint64(n)
	case uint8:
		return uint64(n)
	case int8:
		return uint64(n)
	case float64:
		return uint64(n)
	case float32:
		return uint64(n)
	}
	return 0
}

// Float returns the field at i as float64, or 0 if absent or non-numeric.
func (v Values) Float(i int) float64 {
	if i < 0 || i >= len(v) || v[i] == nil {
		return 0
	}
	switch n := v[i].(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case int32:
		return float64(n)
	case uint32:
		return float64(n)
	case int16:
		return float64(n)
	case uint16:
		return float64(n)
	case int8:
		return float64(n)
	case uint8:
		return float64(n)
	}
	return 0
}

// Str returns the field at i as a string, or "" if absent.
func (v Values) Str(i int) string {
	if i < 0 || i >= len(v) || v[i] == nil {
		return ""
	}
	if s, ok := v[i].(string); ok {
		return s
	}
	return ""
}

// ---------------------------------------------------------------------------
// UTF-16LE helpers
// ---------------------------------------------------------------------------

// EncodeUTF16LE encodes s as UTF-16LE bytes (no terminator).
func EncodeUTF16LE(s string) []byte {
	u := utf16.Encode([]rune(s))
	out := make([]byte, 2*len(u))
	for i, c := range u {
		binary.LittleEndian.PutUint16(out[2*i:2*i+2], c)
	}
	return out
}

// DecodeUTF16LE decodes UTF-16LE bytes, cutting the result at the first
// NUL character as the reference parser does.
func DecodeUTF16LE(b []byte) string {
	n := len(b) / 2
	u := make([]uint16, n)
	for i := 0; i < n; i++ {
		u[i] = binary.LittleEndian.Uint16(b[2*i : 2*i+2])
	}
	s := string(utf16.Decode(u))
	for i, r := range s {
		if r == 0 {
			return s[:i]
		}
	}
	return s
}

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

// fld builds a fixed-size schema field; fldL builds a variable-length
// (string/bytes) schema field of the given byte length.
func fld(t int) Field          { return Field{Type: t} }
func fldL(t, length int) Field { return Field{Type: t, Length: length} }

// AccountSchema ("fl") describes the account record of command 3.
// Size: 816 bytes.
var AccountSchema = Schema{
	fld(TUint8), fld(TInt32), fld(TInt32), fld(TFloat64), fld(TFloat64),
	fldL(TString, 64), fld(TUint32), fld(TUint32), fldL(TString, 256), fld(TUint16),
	fldL(TString, 128), fldL(TString, 256), fld(TInt32), fld(TInt8), fld(TUint32),
	fld(TUint32), fld(TFloat64), fld(TFloat64), fld(TUint32), fld(TFloat64),
	fld(TUint32), fld(TFloat64), fld(TFloat64), fld(TFloat64), fld(TUint32), fld(TUint32),
}

// SymbolSchema ("Mh") describes one record of the compressed symbol
// list of command 34. Size: 526 bytes.
var SymbolSchema = Schema{
	fldL(TString, 64), fldL(TString, 128), fld(TUint32), fld(TUint32),
	fldL(TString, 256), fld(TUint32), fldL(TString, 64), fld(TUint16),
}

// QuoteSchema ("Uh") describes one quote record of command 8.
// Size: 50 bytes.
var QuoteSchema = Schema{
	fld(TUint32), fld(TInt32), fld(TUint32), fld(TFloat64), fld(TFloat64),
	fld(TFloat64), fld(TInt64), fld(TUint32), fld(TUint16),
}

// PositionSchema ("uu") describes one position record of command 4.
// Size: 344 bytes.
var PositionSchema = Schema{
	fld(TInt64), fld(TInt64), fld(TUint32), fld(TUint32), fldL(TString, 64), fld(TUint32),
	fld(TFloat64), fld(TFloat64), fld(TFloat64), fld(TFloat64), fld(TUint64),
	fld(TFloat64), fld(TFloat64), fld(TFloat64), fld(TFloat64), fld(TFloat64),
	fld(TInt64), fld(TInt64), fldL(TString, 64), fld(TFloat64), fld(TUint32),
	fld(TUint32), fld(TUint32), fldL(TString, 64), fld(TInt32), fld(TInt32),
}

// DealSchema ("xd") describes one deal record of command 5.
// Size: 356 bytes.
var DealSchema = Schema{
	fld(TInt64), fldL(TString, 64), fld(TInt64), fld(TUint32), fld(TUint32),
	fldL(TString, 64), fld(TUint32), fld(TUint32), fld(TFloat64), fld(TFloat64),
	fld(TFloat64), fld(TFloat64), fld(TUint64), fld(TFloat64), fld(TFloat64),
	fld(TFloat64), fld(TFloat64), fld(TFloat64), fld(TInt64), fld(TInt64),
	fldL(TString, 64), fld(TFloat64), fld(TUint32), fld(TUint32), fld(TUint32),
	fld(TInt32), fld(TInt32), fld(TFloat64),
}

// OpSchema describes the 248-byte trade request struct ("Op") carried
// by command 12 and embedded in trade events (command 19).
var OpSchema = Schema{
	fld(TUint32), fld(TUint32), fldL(TString, 64), fld(TUint64), fld(TUint32),
	fld(TUint64), fld(TUint32), fld(TUint32), fld(TUint32), fld(TUint32), fld(TUint32),
	fld(TFloat64), fld(TFloat64), fld(TFloat64), fld(TFloat64), fld(TUint32),
	fld(TFloat64), fld(TFloat64), fldL(TString, 64), fld(TUint64), fld(TUint64),
	fld(TUint32),
}

// OpSize is the byte size of one Op record (248).
const OpSize = 248

// OpParams carries every field of an Op trade request. Zero values are
// written as zero, matching the reference's pack_op.
type OpParams struct {
	ActionID       uint32
	TradeAction    uint32
	Symbol         string
	Volume         uint64 // raw wire units (lots * LotMultiplier)
	Digits         uint32
	TradeOrder     uint64
	TradeType      uint32
	TypeFilling    uint32
	TypeTime       uint32
	TypeFlags      uint32
	Price          float64
	PriceTrigger   float64
	SL             float64
	TP             float64
	PriceDeviation uint32
	Comment        string
	TradePosition  uint64
}

// PackOp serializes an Op record. The byte layout is fixed by the
// protocol and reproduced exactly from the reference pack_op,
// including field offsets that the schema order alone would suggest
// differently (comment at 164, trade_position at 228).
func PackOp(p OpParams) []byte {
	op := make([]byte, OpSize)
	put := binary.LittleEndian.PutUint32
	put64 := binary.LittleEndian.PutUint64
	put(op[0:4], p.ActionID)
	put(op[4:8], p.TradeAction)
	sym := EncodeUTF16LE(p.Symbol)
	if len(sym) > 64 {
		sym = sym[:64]
	}
	copy(op[8:72], sym)
	put64(op[72:80], p.Volume)
	put(op[80:84], p.Digits)
	put64(op[84:92], p.TradeOrder)
	put(op[92:96], p.TradeType)
	put(op[96:100], p.TypeFilling)
	put(op[100:104], p.TypeTime)
	put(op[104:108], p.TypeFlags)
	// op[108:112] type_reason = 0
	binary.LittleEndian.PutUint64(op[112:120], math.Float64bits(p.Price))
	binary.LittleEndian.PutUint64(op[120:128], math.Float64bits(p.PriceTrigger))
	binary.LittleEndian.PutUint64(op[128:136], math.Float64bits(p.SL))
	binary.LittleEndian.PutUint64(op[136:144], math.Float64bits(p.TP))
	put(op[144:148], p.PriceDeviation)
	// op[148:164] price_top / price_bottom = 0
	comment := EncodeUTF16LE(p.Comment)
	if len(comment) > 64 {
		comment = comment[:64]
	}
	copy(op[164:228], comment)
	put64(op[228:236], p.TradePosition)
	// op[236:244] position_by = 0, op[244:248] time_expiration = 0
	return op
}

// ---------------------------------------------------------------------------
// Candles (command 11)
// ---------------------------------------------------------------------------

// CandleSize is the byte size of one raw candle record (48).
const CandleSize = 48

// RawCandle is one parsed candle record before time conversion.
type RawCandle struct {
	Timestamp  int32
	Open       float64
	High       float64
	Low        float64
	Close      float64
	TickVolume int64
	Spread     int32
}

// ParseCandles parses a body of back-to-back 48-byte candle records
// (no count prefix), as returned by command 11.
func ParseCandles(body []byte) []RawCandle {
	n := len(body) / CandleSize
	out := make([]RawCandle, 0, n)
	for i := 0; i < n; i++ {
		rec := body[i*CandleSize:]
		out = append(out, RawCandle{
			Timestamp:  int32(binary.LittleEndian.Uint32(rec[0:4])),
			Open:       math.Float64frombits(binary.LittleEndian.Uint64(rec[4:12])),
			High:       math.Float64frombits(binary.LittleEndian.Uint64(rec[12:20])),
			Low:        math.Float64frombits(binary.LittleEndian.Uint64(rec[20:28])),
			Close:      math.Float64frombits(binary.LittleEndian.Uint64(rec[28:36])),
			TickVolume: int64(binary.LittleEndian.Uint64(rec[36:44])),
			Spread:     int32(binary.LittleEndian.Uint32(rec[44:48])),
		})
	}
	return out
}

// EstimateSecondsPerCandle approximates the duration covered by one
// candle of the given timeframe value, used to size history requests.
// It replicates the reference heuristic exactly.
func EstimateSecondsPerCandle(tf int) int {
	switch {
	case tf <= 30:
		return tf * 60
	case tf < 16385:
		return tf * 60
	case tf < 16408:
		return 3600
	case tf < 32769:
		return 86400
	case tf < 49153:
		return 604800
	default:
		return 2592000
	}
}
