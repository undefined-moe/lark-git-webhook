package forwarder

import (
	"strings"
	"testing"

	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

func TestFormatAuditLogUsesSafeDynamicFields(t *testing.T) {
	item := store.Item{Event: "audit_log", RawJSON: []byte(`{"action":"repo.destroy","org":"example-org","actor":"alice","repo":"example-org/private","actor_ip":"192.0.2.1","country":"US","created_at":"2026-01-02T03:04:05Z","target":"https://evil.example"}`)}
	got := Format(item)
	for _, want := range []string{"[audit:repo.destroy] example-org", "actor alice", "target example-org/private", "IP 192.0.2.1", "github_geo US", "2026-01-02T03:04:05Z"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q missing %q", got, want)
		}
	}
	message := MakeMessage([]string{got})
	for _, text := range message.Content.Post["zh_cn"].Content[0] {
		if text.Tag == "a" {
			t.Fatalf("audit record created link: %#v", text)
		}
	}
}

func TestFormatAuditLogGeoFieldsArePlainText(t *testing.T) {
	for _, tc := range []struct {
		name       string
		raw        string
		want       string
		withoutGeo bool
	}{
		{
			name: "object",
			raw:  `{"actor_ip":"192.0.2.1","actor_location":{"country_code":"US","region_name":"California","city":"San Francisco","location_name":"Bay Area"}}`,
			want: "IP 192.0.2.1 | github_geo US, California, San Francisco, Bay Area",
		},
		{
			name: "string",
			raw:  `{"actor_location":"London, UK"}`,
			want: "github_geo London, UK",
		},
		{
			name: "top-level",
			raw:  `{"country":"US","region":"Washington","city":"Seattle"}`,
			want: "github_geo US, Washington, Seattle",
		},
		{
			name:       "missing",
			raw:        `{}`,
			withoutGeo: true,
		},
		{
			name: "malicious-url",
			raw:  `{"actor_location":{"country":"https://evil.example","city":"<script>city</script>"}}`,
			want: "github_geo https://evil.example, <script>city</script>",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Format(store.Item{Event: "audit_log", RawJSON: []byte(tc.raw)})
			if tc.withoutGeo {
				if strings.Contains(got, "github_geo ") {
					t.Fatalf("unexpected GitHub geo in %q", got)
				}
			} else if !strings.Contains(got, tc.want) {
				t.Fatalf("%q missing %q", got, tc.want)
			}
			for _, text := range MakeMessage([]string{got}).Content.Post["zh_cn"].Content[0] {
				if text.Tag == "a" {
					t.Fatalf("audit geo created link: %#v", text)
				}
			}
		})
	}
}

func TestFormatAuditLogIncludesOptionalMaxMindGeo(t *testing.T) {
	for _, tc := range []struct {
		name        string
		raw         string
		lookup      GeoLookup
		want        []string
		notWant     []string
		lookupCalls int
		plainText   bool
	}{
		{
			name: "both sources",
			raw:  `{"actor_ip":"8.8.8.8","actor_location":{"country":"US","city":"Seattle"}}`,
			lookup: func(string) string {
				return "US, Washington, Seattle, America/Los_Angeles"
			},
			want:        []string{"IP 8.8.8.8", "github_geo US, Seattle", "maxmind_geo US, Washington, Seattle, America/Los_Angeles"},
			lookupCalls: 1,
		},
		{
			name: "only MaxMind",
			raw:  `{"actor_ip":"8.8.8.8"}`,
			lookup: func(string) string {
				return "US, Washington, Seattle"
			},
			want:        []string{"maxmind_geo US, Washington, Seattle"},
			notWant:     []string{"github_geo "},
			lookupCalls: 1,
		},
		{
			name:        "only GitHub",
			raw:         `{"actor_location":{"country":"US","city":"Seattle"}}`,
			lookup:      func(string) string { t.Fatal("lookup called without an IP"); return "" },
			want:        []string{"github_geo US, Seattle"},
			notWant:     []string{"maxmind_geo "},
			lookupCalls: 0,
		},
		{
			name: "special IP displays without lookup",
			raw:  `{"actor_ip":"0.1.2.3"}`,
			lookup: func(string) string {
				t.Fatal("lookup called for a special-use IP")
				return ""
			},
			want:        []string{"IP 0.1.2.3"},
			notWant:     []string{"maxmind_geo "},
			lookupCalls: 0,
		},
		{
			name: "invalid IP skips lookup and display",
			raw:  `{"actor_ip":"not-an-ip","country":"US"}`,
			lookup: func(string) string {
				t.Fatal("lookup called for an invalid IP")
				return ""
			},
			want:        []string{"github_geo US"},
			notWant:     []string{"IP ", "maxmind_geo "},
			lookupCalls: 0,
		},
		{
			name: "malicious MaxMind text stays plain",
			raw:  `{"actor_ip":"8.8.8.8"}`,
			lookup: func(string) string {
				return "https://evil.example | <script>city</script>"
			},
			want:        []string{"maxmind_geo https://evil.example / <script>city</script>"},
			lookupCalls: 1,
			plainText:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			lookup := func(ip string) string {
				calls++
				return tc.lookup(ip)
			}
			got := format(store.Item{Event: "audit_log", RawJSON: []byte(tc.raw)}, lookup)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Fatalf("%q missing %q", got, want)
				}
			}
			for _, notWant := range tc.notWant {
				if strings.Contains(got, notWant) {
					t.Fatalf("%q unexpectedly contains %q", got, notWant)
				}
			}
			if calls != tc.lookupCalls {
				t.Fatalf("lookup calls=%d want %d", calls, tc.lookupCalls)
			}
			if tc.plainText {
				for _, segment := range MakeMessage([]string{got}).Content.Post["zh_cn"].Content[0] {
					if segment.Tag == "a" {
						t.Fatalf("MaxMind text created link: %#v", segment)
					}
				}
			}
		})
	}
}
