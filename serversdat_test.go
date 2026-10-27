package mtapi

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/voorz/mtapi/internal/cryptox"
	"github.com/voorz/mtapi/internal/wire"
)

func putStr(block []byte, off int, s string) {
	copy(block[off:], wire.EncodeUTF16LE(s))
}

func putI32(block []byte, off int, v int32) {
	binary.LittleEndian.PutUint32(block[off:off+4], uint32(v))
}

func buildDat(t *testing.T, headerID int32) []byte {
	t.Helper()
	var buf bytes.Buffer
	header := make([]byte, 428)
	putI32(header, 0, headerID)
	putI32(header, 172, 1) // object count
	buf.Write(header)

	ex := headerID >= 505
	infoSize := 660
	if ex {
		infoSize = 1716
	}
	info := make([]byte, infoSize)
	putStr(info, 0, "Exness-MT5Trial17")
	putStr(info, 128, "Exness Technologies Ltd")
	putI32(info, 392, 0)
	putI32(info, 396, 2)
	putStr(info, 404, "mt5trial17.exness.com")
	putI32(info, 532, 42)
	buf.Write(cryptox.XOREncryptBlock(info))

	// One access record with one address.
	binary.Write(&buf, binary.LittleEndian, int32(1))
	acc := make([]byte, 356)
	putStr(acc, 0, "Exness-MT5Trial17")
	putI32(acc, 192, 443)
	buf.Write(cryptox.XOREncryptBlock(acc))
	binary.Write(&buf, binary.LittleEndian, int32(1))
	addr := make([]byte, 148)
	putStr(addr, 0, "192.0.2.55:443")
	buf.Write(cryptox.XOREncryptBlock(addr))

	if ex {
		binary.Write(&buf, binary.LittleEndian, int32(1))
		accEx := make([]byte, 3160)
		putStr(accEx, 0, "Exness")
		putStr(accEx, 512, "Exness-MT5Trial17")
		putStr(accEx, 600, "https://example.invalid")
		putStr(accEx, 856, "/path")
		buf.Write(cryptox.XOREncryptBlock(accEx))
		binary.Write(&buf, binary.LittleEndian, int32(1))
		addrEx := make([]byte, 1284)
		putI32(addrEx, 0, 1)
		putStr(addrEx, 4, "192.0.2.56:443")
		putStr(addrEx, 516, "primary")
		buf.Write(cryptox.XOREncryptBlock(addrEx))
	}
	return buf.Bytes()
}

func TestParseServersDat(t *testing.T) {
	for _, id := range []int32{503, 505} {
		servers, err := ParseServersDat(buildDat(t, id))
		if err != nil {
			t.Fatalf("id %d: %v", id, err)
		}
		if len(servers) != 1 {
			t.Fatalf("id %d: %d servers", id, len(servers))
		}
		s := servers[0]
		if s.Name != "Exness-MT5Trial17" || s.Company != "Exness Technologies Ltd" ||
			s.Timezone != 2 || s.PingTime != 42 || s.Address != "mt5trial17.exness.com" {
			t.Fatalf("id %d: server = %+v", id, s)
		}
		if len(s.Access) != 1 || s.Access[0].Port != 443 || s.Access[0].Addresses[0] != "192.0.2.55:443" {
			t.Fatalf("id %d: access = %+v", id, s.Access)
		}
		points := s.AccessPoints()
		if id == 505 {
			if len(s.AccessEx) != 1 || s.AccessEx[0].Addresses[0].Address != "192.0.2.56:443" {
				t.Fatalf("accessEx = %+v", s.AccessEx)
			}
			if len(points) != 2 {
				t.Fatalf("points = %v", points)
			}
		} else if len(points) != 1 || points[0] != "192.0.2.55:443" {
			t.Fatalf("points = %v", points)
		}
		if found, ok := FindServersDatServer(servers, "exness-mt5trial17"); !ok || found.Name != s.Name {
			t.Fatalf("find = %+v, %v", found, ok)
		}
		info := s.ServerInfo()
		if info.Name != s.Name || len(info.Access) == 0 {
			t.Fatalf("serverInfo = %+v", info)
		}
	}
}

func TestParseServersDatErrors(t *testing.T) {
	if _, err := ParseServersDat([]byte{1, 2, 3}); err == nil {
		t.Fatal("short file must error")
	}
	bad := buildDat(t, 505)
	binary.LittleEndian.PutUint32(bad[0:4], 999)
	if _, err := ParseServersDat(bad); err == nil {
		t.Fatal("unknown format id must error")
	}
	truncated := buildDat(t, 505)[:600]
	if _, err := ParseServersDat(truncated); err == nil {
		t.Fatal("truncated file must error")
	}
}
