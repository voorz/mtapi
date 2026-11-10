package mtapi

import (
	"context"
	"testing"
	"time"
)

func TestProfitFromQuote(t *testing.T) {
	const goldContract = 100.0 // XAUUSDm: 100 oz per lot
	cases := []struct {
		name  string
		pos   Position
		quote Quote
		want  float64
		ok    bool
	}{
		{"long gains", Position{Type: TypeBuy, Price: 4000, Volume: 0.5}, Quote{Bid: 4010, Ask: 4010.2}, 500, true},
		{"long loses", Position{Type: TypeBuy, Price: 4000, Volume: 0.5}, Quote{Bid: 3990, Ask: 3990.2}, -500, true},
		{"short gains", Position{Type: TypeSell, Price: 4000, Volume: 0.5}, Quote{Bid: 3989.8, Ask: 3990}, 500, true},
		{"short loses", Position{Type: TypeSell, Price: 4000, Volume: 2}, Quote{Bid: 4005, Ask: 4005}, -1000, true},
		{"no bid", Position{Type: TypeBuy, Price: 4000, Volume: 1}, Quote{}, 0, false},
		{"no ask", Position{Type: TypeSell, Price: 4000, Volume: 1}, Quote{Bid: 3999}, 0, false},
	}
	for _, tc := range cases {
		got, ok := profitFromQuote(tc.pos, tc.quote, goldContract)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("%s: got (%v, %v), want (%v, %v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
	if _, ok := profitFromQuote(Position{Type: TypeBuy, Price: 1, Volume: 1}, Quote{Bid: 2}, 0); ok {
		t.Error("zero contract size must not recompute")
	}
}

func TestProfitCurrency(t *testing.T) {
	cases := map[string]string{
		"XAUUSDm": "USD",
		"XAUUSD+": "USD",
		"XAUUSD":  "USD",
		"GOLD":    "USD",
		"GOLD#":   "USD",
		"EURUSD":  "USD",
		"EURUSDm": "USD",
		"USDJPY":  "JPY",
		"EURGBP":  "GBP",
		"GBPUSD+": "USD",
		"BTCUSD":  "USD",
		"US500":   "",
		"DE40":    "",
		"":        "",
	}
	for symbol, want := range cases {
		if got := profitCurrency(symbol); got != want {
			t.Errorf("profitCurrency(%q) = %q, want %q", symbol, got, want)
		}
	}
}

// A client holding a quote and a spec for the position's symbol
// recomputes the profit; without either, the server value stands.
// The currency gate keeps non-account-currency symbols untouched.
func TestLivePositionsRecompute(t *testing.T) {
	c := &Client{
		quotes: map[string]Quote{
			"XAUUSDm": {Symbol: "XAUUSDm", Bid: 4010, Ask: 4010.2},
			"USDJPY":  {Symbol: "USDJPY", Bid: 150.10, Ask: 150.12},
		},
		symbols: map[string]*Symbol{
			"XAUUSDm": {Name: "XAUUSDm", ContractSize: 100},
			"USDJPY":  {Name: "USDJPY", ContractSize: 100000},
		},
		symbolByID: map[int64]string{},
		subscribed: map[string]bool{"XAUUSDm": true, "USDJPY": true},
		specTried:  map[string]time.Time{},
		account:    Account{Balance: 5000, Currency: "USD"},
		hasAccount: true,
	}
	in := []Position{
		{Ticket: 1, Symbol: "XAUUSDm", Type: TypeBuy, Volume: 0.5, Price: 4000, Profit: -999},
		{Ticket: 2, Symbol: "USDJPY", Type: TypeBuy, Volume: 1, Price: 150, Profit: 42},
		{Ticket: 3, Symbol: "XAGUSDm", Type: TypeBuy, Volume: 1, Price: 30, Profit: 7},
	}
	out := c.livePositions(context.Background(), in)
	if out[0].Profit != 500 {
		t.Errorf("gold position: got profit %v, want 500 (recomputed)", out[0].Profit)
	}
	if out[1].Profit != 42 {
		t.Errorf("USDJPY position: got profit %v, want 42 (JPY profit currency, server value kept)", out[1].Profit)
	}
	if out[2].Profit != 7 {
		t.Errorf("unquoted position: got profit %v, want 7 (server value kept)", out[2].Profit)
	}
}
