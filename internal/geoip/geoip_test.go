package geoip

import "testing"

func TestLocationStringOmitsEmptyAndDuplicates(t *testing.T) {
	location := Location{Country: "US", Subdivision: "California", City: "California", TimeZone: "America/Los_Angeles"}
	if got, want := location.String(), "US, California, America/Los_Angeles"; got != want {
		t.Fatalf("location=%q want %q", got, want)
	}
}

func TestIsPublicRoutable(t *testing.T) {
	for _, ip := range []string{
		"not-an-ip",
		"127.0.0.1",
		"10.0.0.1",
		"169.254.0.1",
		"224.0.0.1",
		"0.0.0.0",
		"0.1.2.3",
		"100.64.0.1",
		"192.0.0.1",
		"192.0.2.1",
		"198.51.100.1",
		"203.0.113.1",
		"198.18.0.1",
		"240.0.0.1",
		"fc00::1",
		"::1",
		"fe80::1",
		"ff00::1",
		"::",
		"2001:db8::1",
	} {
		if IsPublicRoutable(ParseIP(ip)) {
			t.Fatalf("IsPublicRoutable(%q)=true", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !IsPublicRoutable(ParseIP(ip)) {
			t.Fatalf("IsPublicRoutable(%q)=false", ip)
		}
	}
}

func TestLookupSkipsInvalidAndNonPublicIPs(t *testing.T) {
	reader := &Reader{}
	for _, ip := range []string{"not-an-ip", "127.0.0.1", "10.0.0.1", "192.0.2.1", "fc00::1"} {
		if got := reader.Lookup(ip); got != (Location{}) {
			t.Fatalf("Lookup(%q)=%+v", ip, got)
		}
	}
}

func TestOpenFailure(t *testing.T) {
	if _, err := Open("/definitely/not/a/maxmind-city.mmdb"); err == nil {
		t.Fatal("Open accepted a missing database")
	}
}
