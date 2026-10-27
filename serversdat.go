package mtapi

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"

	"github.com/voorz/mtapi/internal/cryptox"
	"github.com/voorz/mtapi/internal/wire"
)

// This file parses MT5 servers.dat files — the terminal's local
// server directory — for offline server resolution, without the
// broker search endpoints. The format follows the reference parser:
//
//   - a 428-byte plaintext header; the object count is a plaintext
//     int32 at offset 172 and the format id an int32 at offset 0
//     (503/504: 660-byte server records, 505/506: 1716-byte "Ex"
//     records with an extra access section);
//   - each record is an independently XOR-block-encrypted block;
//   - record counts between blocks are plaintext int32s.
//
// Offsets inside the decrypted ServerInfo block are shared by both
// format generations: name at 0 (128 bytes), company at 128 (256),
// timezone fields at 392/396, address at 404 (128), ping time at 532.

// ServersDatServer is one server entry of a servers.dat file.
type ServersDatServer struct {
	Name     string
	Company  string
	Address  string // the server's own address string, as stored
	Timezone int32
	DST      int32
	PingTime int32
	Access   []ServersDatAccess
	AccessEx []ServersDatAccessEx
}

// ServersDatAccess is one access-point group of a server.
type ServersDatAccess struct {
	ServerName string
	Port       int32
	Addresses  []string
}

// ServersDatAccessEx is the extended access record of the 505/506
// format generation.
type ServersDatAccessEx struct {
	Name       string
	Host       string
	Path       string
	ServerName string
	Addresses  []ServersDatAddressEx
}

// ServersDatAddressEx is one address of an extended access record.
type ServersDatAddressEx struct {
	Type        int32
	Address     string
	Description string
}

// AccessPoints flattens every address this server entry lists, in
// file order.
func (s ServersDatServer) AccessPoints() []string {
	var out []string
	for _, a := range s.Access {
		out = append(out, a.Addresses...)
	}
	for _, a := range s.AccessEx {
		for _, addr := range a.Addresses {
			out = append(out, addr.Address)
		}
	}
	return out
}

// ServerInfo converts the entry to the ServerInfo used by the rest of
// the package.
func (s ServersDatServer) ServerInfo() ServerInfo {
	return ServerInfo{
		Company: s.Company,
		Name:    s.Name,
		Access:  s.AccessPoints(),
	}
}

// FindServersDatServer looks up a server by name (case-insensitive)
// in parsed servers.dat entries.
func FindServersDatServer(servers []ServersDatServer, name string) (ServersDatServer, bool) {
	for _, s := range servers {
		if strings.EqualFold(s.Name, name) {
			return s, true
		}
	}
	return ServersDatServer{}, false
}

const (
	serversDatHeaderSize  = 428
	serversDatCountOffset = 172
	serverInfoSize        = 660
	serverInfoExSize      = 1716
	accessRecSize         = 356
	addressRecSize        = 148
	accessRecExSize       = 3160
	addressRecExSize      = 1284
)

type datReader struct {
	data []byte
	pos  int
}

func (r *datReader) int32() (int32, error) {
	if r.pos+4 > len(r.data) {
		return 0, fmt.Errorf("mtapi: servers.dat truncated at offset %d", r.pos)
	}
	v := int32(binary.LittleEndian.Uint32(r.data[r.pos : r.pos+4]))
	r.pos += 4
	return v, nil
}

func (r *datReader) encrypted(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, fmt.Errorf("mtapi: servers.dat truncated at offset %d", r.pos)
	}
	block := cryptox.XORDecryptBlock(r.data[r.pos : r.pos+n])
	r.pos += n
	return block, nil
}

// datString decodes a UTF-16LE field exactly like the reference:
// scan for the NUL terminator pair at even offsets, then decode.
func datString(block []byte, off, maxBytes int) string {
	if off >= len(block) {
		return ""
	}
	end := off + maxBytes
	if end > len(block) {
		end = len(block)
	}
	raw := block[off:end]
	for i := 0; i+1 < len(raw); i += 2 {
		if raw[i] == 0 && raw[i+1] == 0 {
			raw = raw[:i]
			break
		}
	}
	return wire.DecodeUTF16LE(raw)
}

func datInt32(block []byte, off int) int32 {
	if off+4 > len(block) {
		return 0
	}
	return int32(binary.LittleEndian.Uint32(block[off : off+4]))
}

func (r *datReader) accessList() ([]ServersDatAccess, error) {
	count, err := r.int32()
	if err != nil {
		return nil, err
	}
	if count < 0 || count > 10000 {
		return nil, fmt.Errorf("mtapi: servers.dat: implausible access count %d", count)
	}
	out := make([]ServersDatAccess, 0, count)
	for i := int32(0); i < count; i++ {
		rec, err := r.encrypted(accessRecSize)
		if err != nil {
			return nil, err
		}
		acc := ServersDatAccess{
			ServerName: datString(rec, 0, 64),
			Port:       datInt32(rec, 192),
		}
		addrCount, err := r.int32()
		if err != nil {
			return nil, err
		}
		if addrCount < 0 || addrCount > 10000 {
			return nil, fmt.Errorf("mtapi: servers.dat: implausible address count %d", addrCount)
		}
		for j := int32(0); j < addrCount; j++ {
			addr, err := r.encrypted(addressRecSize)
			if err != nil {
				return nil, err
			}
			acc.Addresses = append(acc.Addresses, datString(addr, 0, 128))
		}
		out = append(out, acc)
	}
	return out, nil
}

func (r *datReader) accessExList() ([]ServersDatAccessEx, error) {
	count, err := r.int32()
	if err != nil {
		return nil, err
	}
	if count < 0 || count > 10000 {
		return nil, fmt.Errorf("mtapi: servers.dat: implausible access-ex count %d", count)
	}
	out := make([]ServersDatAccessEx, 0, count)
	for i := int32(0); i < count; i++ {
		rec, err := r.encrypted(accessRecExSize)
		if err != nil {
			return nil, err
		}
		acc := ServersDatAccessEx{
			Name:       datString(rec, 0, 128),
			Host:       datString(rec, 600, 256),
			Path:       datString(rec, 856, 2048),
			ServerName: datString(rec, 512, 64),
		}
		addrCount, err := r.int32()
		if err != nil {
			return nil, err
		}
		if addrCount < 0 || addrCount > 10000 {
			return nil, fmt.Errorf("mtapi: servers.dat: implausible address-ex count %d", addrCount)
		}
		for j := int32(0); j < addrCount; j++ {
			addr, err := r.encrypted(addressRecExSize)
			if err != nil {
				return nil, err
			}
			acc.Addresses = append(acc.Addresses, ServersDatAddressEx{
				Type:        datInt32(addr, 0),
				Address:     datString(addr, 4, 512),
				Description: datString(addr, 516, 512),
			})
		}
		out = append(out, acc)
	}
	return out, nil
}

// ParseServersDat parses the contents of a servers.dat file.
func ParseServersDat(data []byte) ([]ServersDatServer, error) {
	if len(data) < serversDatHeaderSize {
		return nil, fmt.Errorf("mtapi: servers.dat too short: %d bytes", len(data))
	}
	headerID := int32(binary.LittleEndian.Uint32(data[0:4]))
	if headerID != 503 && headerID != 504 && headerID != 505 && headerID != 506 {
		return nil, fmt.Errorf("mtapi: unknown servers.dat format id %d", headerID)
	}
	ex := headerID >= 505
	objCount := int32(binary.LittleEndian.Uint32(data[serversDatCountOffset : serversDatCountOffset+4]))
	if objCount < 0 || objCount > 100000 {
		return nil, fmt.Errorf("mtapi: servers.dat: implausible object count %d", objCount)
	}

	r := &datReader{data: data, pos: serversDatHeaderSize}
	servers := make([]ServersDatServer, 0, objCount)
	for i := int32(0); i < objCount; i++ {
		size := serverInfoSize
		if ex {
			size = serverInfoExSize
		}
		info, err := r.encrypted(size)
		if err != nil {
			return nil, err
		}
		srv := ServersDatServer{
			Name:     datString(info, 0, 128),
			Company:  datString(info, 128, 256),
			DST:      datInt32(info, 392),
			Timezone: datInt32(info, 396),
			Address:  datString(info, 404, 128),
			PingTime: datInt32(info, 532),
		}
		if srv.Access, err = r.accessList(); err != nil {
			return nil, err
		}
		if ex {
			if srv.AccessEx, err = r.accessExList(); err != nil {
				return nil, err
			}
		}
		servers = append(servers, srv)
	}
	return servers, nil
}

// LoadServersDat reads and parses a servers.dat file from disk.
func LoadServersDat(path string) ([]ServersDatServer, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseServersDat(data)
}
