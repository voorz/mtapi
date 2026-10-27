package mtapi

import "time"

// Position and order types, as used by Position.Type, Order.Type,
// Deal.Type and the trade methods.
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

// RetcodeDone (10009) is the trade retcode for a request completed
// successfully.
const RetcodeDone = 10009

// RetcodeDonePartial reports a partially completed trade request
// (for example a close filled for part of the requested volume).
const RetcodeDonePartial = 10010

// RetcodeRequote reports that the price moved before an
// instant-execution order could be filled; nothing was executed and
// the request may be resubmitted with a fresh quote.
const RetcodeRequote = 10004

// RetcodeInvalid reports that the server rejected the request
// parameters; nothing was executed.
const RetcodeInvalid = 10013

// Account is a snapshot of the trading account (command 3).
//
// Login is the account number the client authenticated with: the
// account record itself does not echo it (verified against a live
// server), and the record's first field is an unrelated internal id.
//
// FreeMargin is part of the data model but is not present in the
// account record the protocol returns; it stays zero, as in the
// reference implementation.
type Account struct {
	Login      int64
	Balance    float64
	Equity     float64
	Margin     float64
	FreeMargin float64
	Currency   string
	Group      string
	Leverage   int
	Server     string
	Profit     float64
}

// Symbol describes one tradable instrument. The symbol list (command
// 34) fills Name, ID, Digits and CalcMode; the remaining fields are
// filled when the server pushes the symbol's full specification
// (command 17), which happens for subscribed symbols.
type Symbol struct {
	Name              string
	ID                int64
	Digits            int
	Point             float64
	Spread            int
	TradeMode         int
	ContractSize      float64
	TickValue         float64
	TickSize          float64
	CalcMode          int
	MinVolume         int64 // raw wire units (see wire.LotMultiplier)
	MaxVolume         int64
	VolumeStep        int64
	SwapLong          float64
	SwapShort         float64
	InitialMargin     float64
	MaintenanceMargin float64
}

// Quote is the latest bid/ask for a symbol. RawBid and RawAsk are the
// integer prices on the wire; Bid and Ask are RawBid / 10^Digits.
// Time is the local arrival time, as in the reference implementation.
type Quote struct {
	Symbol   string
	SymbolID int64
	Bid      float64
	Ask      float64
	RawBid   int64
	RawAsk   int64
	Time     time.Time
}

// Position is one open position. Ticket is the ORDER ticket that
// opened the position — it is the identifier every position operation
// (close, modify) takes. Volume is in lots.
type Position struct {
	Ticket     int64
	Symbol     string
	Type       int // TypeBuy or TypeSell
	Volume     float64
	Price      float64 // open price
	SL         float64
	TP         float64
	Profit     float64
	Swap       float64
	Commission float64
	Comment    string
	Time       time.Time
}

// Order is one pending order. The protocol's order record fills only
// Ticket, Symbol, Type, Volume and Price; the remaining fields stay
// zero, as in the reference implementation.
type Order struct {
	Ticket  int64
	Symbol  string
	Type    int // TypeBuyLimit .. TypeSellStopLimit
	Status  int
	Volume  float64 // lots
	Price   float64
	SL      float64
	TP      float64
	Comment string
	Time    time.Time
}

// Deal is one executed deal from the account history. Position is part
// of the data model but is not filled by the reference parser and
// stays zero.
type Deal struct {
	Ticket     int64
	Order      int64 // order ticket that produced the deal
	Position   int64
	Symbol     string
	Type       int
	Volume     float64 // lots
	Price      float64
	Profit     float64
	Commission float64
	Swap       float64
	Comment    string
	Time       time.Time
}

// Candle is one OHLC bar.
type Candle struct {
	Time       time.Time
	Timestamp  int64 // unix seconds, as on the wire
	Open       float64
	High       float64
	Low        float64
	Close      float64
	TickVolume int64
	Spread     int
}

// TradeResult is the outcome of a trade operation. Volume is in lots;
// RawVolume is the same quantity in raw wire units. Retcode is the
// MT5 retcode (RetcodeDone on success).
type TradeResult struct {
	Retcode   int
	Deal      int64
	Order     int64
	Position  int64
	Volume    float64
	RawVolume int64
	Price     float64
	Comment   string
}

// TradeEvent is a trade execution report pushed by the server
// (command 19). It carries the same fields as TradeResult.
type TradeEvent struct {
	Retcode   int
	Deal      int64
	Order     int64
	Position  int64
	Volume    float64 // lots
	RawVolume int64
	Price     float64
	Comment   string
}

// SymbolSpec is a full symbol specification pushed by the server
// (command 17). The fields are also merged into the client's Symbol
// for the symbol. Raw holds the complete 1696-byte spec record for
// callers that need fields this package does not decode.
type SymbolSpec struct {
	Name         string
	TickValue    float64
	TickSize     float64
	ContractSize float64
	Digits       int
	Point        float64
	Spread       int
	CalcMode     int
	Raw          []byte
}

// ServerInfo describes one trading server found by broker search.
type ServerInfo struct {
	Company string
	Name    string
	Site    string
	Access  []string // access points, "ip:port"
	IsDemo  bool
}

// Timeframe identifies a candle period. The values are the protocol's
// own timeframe codes.
type Timeframe int

// Timeframe codes.
const (
	TFM1  Timeframe = 1
	TFM5  Timeframe = 5
	TFM15 Timeframe = 15
	TFM30 Timeframe = 30
	TFH1  Timeframe = 16385
	TFH4  Timeframe = 16388
	TFD1  Timeframe = 16408
	TFW1  Timeframe = 32769
	TFMN1 Timeframe = 49153
)

// String returns the conventional timeframe label ("M1", "H1", ...).
func (tf Timeframe) String() string {
	switch tf {
	case TFM1:
		return "M1"
	case TFM5:
		return "M5"
	case TFM15:
		return "M15"
	case TFM30:
		return "M30"
	case TFH1:
		return "H1"
	case TFH4:
		return "H4"
	case TFD1:
		return "D1"
	case TFW1:
		return "W1"
	case TFMN1:
		return "MN1"
	}
	return "TF?"
}
