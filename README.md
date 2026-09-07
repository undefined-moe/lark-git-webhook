# lark-git-webhook

`lark-git-webhook` is a Linux-only, self-hosted GitHub webhook-to-Lark App IM API relay. It runs as one static Go binary with one local bbolt database; no Redis, Kafka, external database, or installer script is required.

## Build

```sh
CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /tmp/lark-git-webhook ./cmd/lark-git-webhook
```

## Install with systemd

The supplied unit publishes the GitHub webhook on port 8080 and keeps operational endpoints on loopback port 9090.

```sh
sudo install -Dm0755 /tmp/lark-git-webhook /usr/local/bin/lark-git-webhook
sudo install -Dm0644 deploy/lark-git-webhook.service /etc/systemd/system/lark-git-webhook.service
sudo install -Dm0600 /dev/null /etc/lark-git-webhook.env
sudoedit /etc/lark-git-webhook.env
```

Create `/etc/lark-git-webhook.env` with only the secrets:

```sh
GITHUB_WEBHOOK_SECRET='your-github-secret'
LARK_APP_ID='cli_your_lark_app_id'
LARK_APP_SECRET='your-lark-app-secret'
LARK_CHAT_ID='oc_your_lark_chat_id'
LARK_VERIFICATION_TOKEN='your-lark-verification-token'
# Optional public paths; these defaults must remain distinct.
WEBHOOK_PATH='/webhook'
LARK_EVENT_PATH='/webhook/lark/event'
LARK_CALLBACK_PATH='/webhook/lark/callback'
# Optional; defaults to 8 simultaneous webhook requests.
WEBHOOK_MAX_IN_FLIGHT='8'
# Optional; defaults to 1073741824 (1 GiB).
ARCHIVE_MIN_FREE_BYTES='1073741824'
# Optional GitHub organization audit-log polling. Set both to enable; default is disabled.
# Fine-grained tokens require Administration:read; classic tokens require read:audit_log.
GITHUB_AUDIT_ORG='example-org'
GITHUB_AUDIT_TOKEN='replace-with-a-token'
# Optional; defaults to 1m and must be at least 10s when audit polling is enabled.
GITHUB_AUDIT_POLL_INTERVAL='1m'
# Optional offline MaxMind GeoLite2-City or GeoIP2-City database; empty disables it.
MAXMIND_CITY_DB_PATH='/root/lark-git-webhook/data/GeoLite2-City.mmdb'
```

```sh
sudo chmod 600 /etc/lark-git-webhook.env
sudo systemctl daemon-reload
sudo systemctl enable --now lark-git-webhook
sudo systemctl status lark-git-webhook
```

The unit uses `DynamicUser=yes`, `StateDirectory=lark-git-webhook`, and `DATA_DIR=/var/lib/lark-git-webhook`. It sets `LISTEN_ADDR=0.0.0.0:8080` for the public webhook, `WEBHOOK_PATH=/webhook`, `LARK_EVENT_PATH=/webhook/lark/event`, `LARK_CALLBACK_PATH=/webhook/lark/callback`, and `ADMIN_LISTEN_ADDR=127.0.0.1:9090` for administration. Startup rejects a non-loopback admin address and public paths that are invalid, root, or overlapping.

Put a TLS reverse proxy in front of the configured public paths (for example `http://127.0.0.1:8080/webhook`, `http://127.0.0.1:8080/webhook/lark/event`, and `http://127.0.0.1:8080/webhook/lark/callback`). Configure GitHub with the matching public Payload URL, content type `application/json`, SSL verification enabled, the matching secret, and **Send me everything**.

When audit polling is first enabled, the service stores the current time as its checkpoint and monitors only subsequent audit records; it never replays historical records. Audit records are durably archived and use the normal outbound Lark queue, batching, and rate limits.

### Optional offline MaxMind City database

Set `MAXMIND_CITY_DB_PATH` to a local GeoLite2-City or GeoIP2-City `.mmdb` file to add an offline `maxmind_geo` field to audit messages alongside GitHub's native `github_geo` field. An empty value disables MaxMind lookups. The database is subject to MaxMind licensing and is not included in the binary or repository; a recommended standalone location is `/root/lark-git-webhook/data/GeoLite2-City.mmdb`. Keep its directory private and the database readable only by the service account, for example directory mode `0700` and file mode `0600` or `0640`.

The supplied unit uses `DynamicUser=yes` and `ProtectHome=yes`, so it cannot read the recommended `/root` path directly. For that unit, use a private file below `/var/lib/lark-git-webhook` or a dedicated read-only bind mount, then set `MAXMIND_CITY_DB_PATH` to that readable location.

The webhook listener serves only `POST` on `WEBHOOK_PATH`. `X-GitHub-Delivery` permits up to 128 safe identifier characters; `X-GitHub-Event` permits up to 64 letters, digits, `_`, or `-`.

### Lark inbound events and callbacks

In the Lark developer console, set the app's Event Subscription request URL to the public `LARK_EVENT_PATH` URL and add `im.chat.access_event.bot_p2p_chat_entered_v1` and `im.message.receive_v1`. Configure the app's card callback URL as the public `LARK_CALLBACK_PATH` URL, and enter the same `LARK_VERIFICATION_TOKEN` in the console and service environment. The bot needs the existing `im:message:send_as_bot` permission; enable the Lark message-read permission required for `im.message.receive_v1`. The chat-entered event needs no additional permission.

When a user enters a bot P2P chat, the service sends that `open_id` one GitHub username form. Lark can repeat the entered event, but the durable local state prevents additional forms. A submitted form is checked against the message that was sent to that user, then validated with one unauthenticated GitHub public API lookup. A valid response stores the canonical GitHub login permanently; the same form can be resubmitted to replace it. Identical callback deliveries replay their durable toast result without a second lookup, and different submissions are limited to one external validation per `open_id` per minute. Invalid or nonexistent usernames return an error toast, while temporary GitHub failures (including the public API's 60 requests/hour/IP limit) return a retryable error and do not change the mapping. Other card callbacks return `{}` after archival.

Both Lark endpoints accept only unencrypted Schema 2.0 JSON deliveries. Leave the Lark Encrypt Key unset: encryption is intentionally unsupported because this production service has no Encrypt Key configured. URL-verification challenges are authenticated and returned but are not archived. Authenticated events and callbacks are durably archived privately. In any group that contains the bot, any member can @ the bot and send the exact command `/github-add owner/repo` to persistently subscribe that group to the normalized lowercase repository name. Commands are idempotent and, after a successful persistent subscription, the service best-effort replies that the normalized repository was subscribed. Each delivery claims at most one reply attempt; a reply failure does not affect the subscription or cause retrying deliveries to send another reply. Only group text commands with a valid `owner/repo` name are accepted. `LARK_CHAT_ID` remains the legacy default group and receives every GitHub event. Onboarding state permanently contains Lark `open_id`, form message IDs, and bound GitHub logins; protect the database and archive with the same restricted backup access. This onboarding change requires no Caddy or environment-variable changes.

## Operations

```sh
curl -fsS http://127.0.0.1:9090/livez
curl -fsS http://127.0.0.1:9090/readyz
curl -fsS http://127.0.0.1:9090/metrics
journalctl -u lark-git-webhook -f
```

Do not expose port 9090 in a public firewall rule or proxy. GitHub deliveries are durable before `202`; authenticated Lark events and callbacks are durable before `200`. Delivery is at least once, and outbound Lark limits are preserved across restarts at 5/s and 100/min. If `/readyz` stays unavailable after a queue-state persistence failure, restart the service after fixing the underlying storage issue. A Lark `Retry-After`, including the transient `99991400` response code, creates a persistent cooldown. HTTP error responses are decoded as Lark envelopes; invalid app credentials (including code `10014`) are permanent. The app exchanges `LARK_APP_ID` and `LARK_APP_SECRET` for a tenant access token and caches it until one minute before expiry; tokens are refreshed after an authorization failure.

Deduplication is best-effort within `DEDUPE_TTL`: reaching `DEDUPE_MAX_ITEMS` evicts earliest-expiring records, so strict TTL-wide deduplication is not guaranteed. Queue overflow returns `503`; **GitHub does not automatically retry failed webhook deliveries**. After recovery, redeliver from the GitHub UI or API.

Normal GitHub events retain their existing post formatting and batching. App IM post content is the locale map expected by Lark (for example `{"zh_cn": ...}`), not an additional `post` wrapper. `workflow_run` and `check_run` maintain one durable interactive card per GitHub repository ID and check-suite ID and update that card in place. Cards opt into `update_multi`, render the workflow attempt, and use the run ID, branch, SHA, and check timestamps to avoid stale updates. A rerun with a higher attempt clears its prior checks and conclusion. Completed runs update the card before adding the status reaction: `success` → `DONE`; `failure`, `timed_out`, and `action_required` → `ERROR`; `cancelled`, `skipped`, `neutral`, and `stale` → `CrossMark`. A queued or in-progress rerun removes a prior reaction; a completed failure adds `ERROR`. `workflow_job` and `check_suite` payloads are permanently archived but intentionally do not send a Lark message. The `/metrics` endpoint includes workflow card and reaction counters.

Permanent Lark 4xx responses (except 408, 409, 425, and 429), and retryable deliveries reaching `MAX_ATTEMPTS`, enter persistent dead-letter storage. Active queue capacity accounts for immutable delivery content plus fixed retry metadata reserve, so a full queue can still persist retry attempts, next-attempt times, and eventual dead-letter transitions. Requeueing changes the permanent delivery state back to queued in the same transaction. Correct the cause, then requeue one entry:

```sh
sudo systemctl stop lark-git-webhook
sudo sh -c 'set -a; . /etc/lark-git-webhook.env; DATA_DIR=/var/lib/lark-git-webhook; export DATA_DIR; exec /usr/local/bin/lark-git-webhook requeue-dead <sequence>'
sudo systemctl start lark-git-webhook
```

A failed requeue exits nonzero and retains the dead-letter entry.

### Permanent payload archive

After GitHub signature, header, and JSON validation, each payload is permanently archived before it is queued at `${DATA_DIR}/archive/<base64url-delivery-prefix>/<base64url-delivery>/payload.<event>.json`. Archive directories are mode `0700` and payload files are mode `0600`. The archive is append-only by delivery ID: a directory may contain only one event/payload, an identical redelivery is safe, and a conflicting event or payload returns `409` without replacing retained data.

A permanent local delivery state records admissions that still need archival. On startup and periodically afterward, the service archives and unlocks those queued deliveries in small batches, so crashes between database admission and file publication cannot silently lose a delivery. The archive grows permanently and can contain repository and sender data. Include it in encrypted, access-restricted backups and monitor its capacity. It does not reconstruct deliveries received before this version was deployed.

The service permanently retains archive files but reserves `ARCHIVE_MIN_FREE_BYTES` of filesystem capacity before accepting another payload. Its default is 1 GiB; the check conservatively includes the archive payload plus worst-case queue, delivery-state, bbolt page, and metadata admission overhead. When free space cannot cover that reserve and admission requirement, webhook requests return `503` without queueing. Filesystem quotas remain the recommended external backstop. `WEBHOOK_MAX_IN_FLIGHT` defaults to 8 and immediately returns `503` for requests above that concurrent limit, before their bodies are read. To inspect inventory and permissions without printing payloads:

```sh
sudo find /var/lib/lark-git-webhook/archive -type f -printf '%p %s bytes\n'
sudo stat -c '%a %U:%G %n' /var/lib/lark-git-webhook/archive /var/lib/lark-git-webhook/archive/*
```

If a payload must be examined for troubleshooting, use a protected JSON viewer rather than copying it into shell history or logs:

```sh
sudo python3 -m json.tool /var/lib/lark-git-webhook/archive/YW/YWItZGVsaXZlcnk/payload.push.json
```

For push troubleshooting, first check `lark_git_webhook_events_received_total{event="push"}` and the archived `payload.push.json` files to confirm GitHub sent the event. The Lark message shows the branch or ref, abbreviated before/after SHA range, commit count, sanitized head-commit message, sender, and GitHub compare link. Branch/ref and commit details are included when GitHub provides them.

## Upgrade and backup

```sh
sudo systemctl stop lark-git-webhook
sudo cp -a /var/lib/lark-git-webhook/lark-git-webhook.db /var/lib/lark-git-webhook/archive /var/backups/lark-git-webhook.$(date +%F-%H%M%S)
sudo install -m0755 /path/to/new/lark-git-webhook /usr/local/bin/lark-git-webhook
sudo systemctl start lark-git-webhook
sudo systemctl status lark-git-webhook
```

To restore, stop the service, replace `/var/lib/lark-git-webhook/lark-git-webhook.db` and `/var/lib/lark-git-webhook/archive` with verified backups, then start it. Restrict access to the database, archive backups, and `/etc/lark-git-webhook.env`.
