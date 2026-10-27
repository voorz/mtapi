package mtapi

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/voorz/mtapi/internal/wire"
)

// orderRecordSize is the fixed byte size of one pending-order record
// inside the command-4 response.
const orderRecordSize = 356

// Positions fetches the open positions (command 4).
func (c *Client) Positions(ctx context.Context) ([]Position, error) {
	resp, err := c.sendAndWait(ctx, wire.CmdPositions, nil, wire.CmdPositions)
	if err != nil {
		return nil, err
	}
	return parsePositions(resp.Body), nil
}

// Orders fetches the pending orders (command 4, the section after the
// positions in the same response).
func (c *Client) Orders(ctx context.Context) ([]Order, error) {
	resp, err := c.sendAndWait(ctx, wire.CmdPositions, nil, wire.CmdPositions)
	if err != nil {
		return nil, err
	}
	return parseOrders(resp.Body), nil
}

// parsePositions parses the position section of a command-4 body:
// [pos_count:u32][positions][order_count:u32][orders].
func parsePositions(body []byte) []Position {
	if len(body) < 4 {
		return nil
	}
	count := binary.LittleEndian.Uint32(body[0:4])
	size := wire.PositionSchema.Size()
	off := 4
	out := make([]Position, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+size > len(body) {
			break
		}
		vals, next := wire.ParseSeries(body, wire.PositionSchema, off)
		off = next
		out = append(out, Position{
			Ticket:     vals.Int(0),
			Symbol:     vals.Str(4),
			Type:       int(vals.Int(5)),
			Volume:     wire.VolumeToLots(vals.Uint(10)),
			Price:      vals.Float(6),
			SL:         vals.Float(8),
			TP:         vals.Float(9),
			Profit:     vals.Float(11),
			Swap:       vals.Float(15),
			Commission: vals.Float(14),
			Comment:    vals.Str(18),
			Time:       timeFromUnix(vals.Int(2)),
		})
	}
	return out
}

// parseOrders parses the order section of a command-4 body, located
// after all position records and an order count. Order records have
// their own fixed 356-byte layout (not a schema record); the field
// offsets follow the reference parser.
func parseOrders(body []byte) []Order {
	if len(body) < 4 {
		return nil
	}
	posCount := binary.LittleEndian.Uint32(body[0:4])
	off := 4 + int(posCount)*wire.PositionSchema.Size()
	if off+4 > len(body) {
		return nil
	}
	count := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	out := make([]Order, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+orderRecordSize > len(body) {
			break
		}
		rec := body[off : off+orderRecordSize]
		out = append(out, Order{
			Ticket: int64(binary.LittleEndian.Uint64(rec[0:8])),
			Symbol: wire.DecodeUTF16LE(rec[72:136]),
			Type:   int(binary.LittleEndian.Uint32(rec[148:152])),
			Volume: wire.VolumeToLots(uint64(binary.LittleEndian.Uint32(rec[204:208]))),
			Price:  f64At(rec, 336),
		})
		off += orderRecordSize
	}
	return out
}

// Deals fetches the deal history (command 5) between from and to. A
// zero from means "from the beginning"; a zero to means "until now".
func (c *Client) Deals(ctx context.Context, from, to time.Time) ([]Deal, error) {
	var fromSec, toSec uint32
	if !from.IsZero() {
		fromSec = uint32(from.Unix())
	}
	if to.IsZero() {
		toSec = uint32(time.Now().Unix())
	} else {
		toSec = uint32(to.Unix())
	}
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload[0:4], fromSec)
	binary.LittleEndian.PutUint32(payload[4:8], toSec)

	resp, err := c.sendAndWait(ctx, wire.CmdDeals, payload, wire.CmdDeals)
	if err != nil {
		return nil, err
	}
	return parseDeals(resp.Body), nil
}

// parseDeals parses a command-5 body: [count:u32][deals].
func parseDeals(body []byte) []Deal {
	if len(body) < 4 {
		return nil
	}
	count := binary.LittleEndian.Uint32(body[0:4])
	size := wire.DealSchema.Size()
	off := 4
	out := make([]Deal, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+size > len(body) {
			break
		}
		vals, next := wire.ParseSeries(body, wire.DealSchema, off)
		off = next
		out = append(out, Deal{
			Ticket:     vals.Int(0),
			Order:      vals.Int(2),
			Symbol:     vals.Str(5),
			Type:       int(vals.Int(7)),
			Volume:     wire.VolumeToLots(vals.Uint(12)),
			Price:      vals.Float(8),
			Profit:     vals.Float(13),
			Commission: vals.Float(16),
			Swap:       vals.Float(17),
			Comment:    vals.Str(20),
			Time:       timeFromUnix(vals.Int(3)),
		})
	}
	return out
}
