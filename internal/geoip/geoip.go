package geoip

import (
	"net"
	"strings"

	"github.com/oschwald/geoip2-golang"
)

// Location is the non-coordinate portion of a MaxMind City record.
type Location struct {
	Country     string
	Subdivision string
	City        string
	TimeZone    string
}

// String returns a compact, de-duplicated plain-text location.
func (l Location) String() string {
	values := []string{l.Country, l.Subdivision, l.City, l.TimeZone}
	unique := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		duplicate := false
		for _, seen := range unique {
			if value == seen {
				duplicate = true
				break
			}
		}
		if !duplicate {
			unique = append(unique, value)
		}
	}
	return strings.Join(unique, ", ")
}

// ParseIP parses an IP address after trimming surrounding whitespace.
func ParseIP(value string) net.IP {
	return net.ParseIP(strings.TrimSpace(value))
}

// IsPublicRoutable reports whether ip is eligible for a MaxMind lookup.
func IsPublicRoutable(ip net.IP) bool {
	if ip == nil || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		switch {
		case ipv4[0] == 0: // this network 0.0.0.0/8
			return false
		case ipv4[0] == 100 && ipv4[1] >= 64 && ipv4[1] <= 127: // CGNAT 100.64.0.0/10
			return false
		case ipv4[0] == 192 && ipv4[1] == 0 && ipv4[2] == 0: // IETF protocol assignments 192.0.0.0/24
			return false
		case ipv4[0] == 192 && ipv4[1] == 0 && ipv4[2] == 2: // TEST-NET-1
			return false
		case ipv4[0] == 198 && ipv4[1] == 51 && ipv4[2] == 100: // TEST-NET-2
			return false
		case ipv4[0] == 203 && ipv4[1] == 0 && ipv4[2] == 113: // TEST-NET-3
			return false
		case ipv4[0] == 198 && (ipv4[1] == 18 || ipv4[1] == 19): // benchmarking 198.18.0.0/15
			return false
		case ipv4[0] >= 240:
			return false
		}
		return true
	}
	ipv6 := ip.To16()
	if ipv6 == nil {
		return false
	}
	return !(ipv6[0] == 0x20 && ipv6[1] == 0x01 && ipv6[2] == 0x0d && ipv6[3] == 0xb8) // documentation 2001:db8::/32
}

// Reader provides offline lookups from a MaxMind City database.
type Reader struct {
	reader *geoip2.Reader
}

// Open opens a GeoLite2-City or GeoIP2-City database.
func Open(path string) (*Reader, error) {
	reader, err := geoip2.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{reader: reader}, nil
}

// Lookup returns an empty location for invalid, non-public, unrecorded, or failed lookups.
func (r *Reader) Lookup(ip string) Location {
	parsed := ParseIP(ip)
	if r == nil || r.reader == nil || !IsPublicRoutable(parsed) {
		return Location{}
	}
	record, err := r.reader.City(parsed)
	if err != nil {
		return Location{}
	}
	location := Location{
		Country:  record.Country.IsoCode,
		City:     record.City.Names["en"],
		TimeZone: record.Location.TimeZone,
	}
	if location.Country == "" {
		location.Country = record.Country.Names["en"]
	}
	if len(record.Subdivisions) > 0 {
		location.Subdivision = record.Subdivisions[0].Names["en"]
	}
	return location
}

// Close releases the database resources.
func (r *Reader) Close() error {
	return r.reader.Close()
}
