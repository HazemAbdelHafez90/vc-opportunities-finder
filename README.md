# Fairpicture Opportunity Finder

Internal Fairpicture tool for discovering media-related opportunities from:

- ReliefWeb
- UNDP Procurement
- UNGM
- ICIMOD
- Welthungerhilfe
- Plan International
- CAF (Confédération Africaine de Football)

The app now uses Supabase as its cache and read layer:

- page load reads cached open opportunities from Supabase
- the UI shows `Last synced X ago`
- clicking `Refresh Results` fetches live sources, recalculates fit scores, upserts into Supabase, and reloads the table
- expired items stay in the database with `status = expired` and are hidden from the main UI
- browser access is protected with Supabase email/password sign-in
- API routes require a valid Supabase session token and can optionally enforce a team email allowlist

## Architecture

Frontend:
- `index.html`
- `app.js`
- `styles.css`

API routes:
- `api/opportunities.py`
  reads cached open rows from Supabase
- `api/sync-status.py`
  returns the latest sync metadata
- `api/refresh.py`
  fetches live data from all sources and updates Supabase
- `api/notification-settings.py`
  reads and saves email notification settings for the sync job
- `api/test-notification.py`
  sends a manual Postmark test email using the current notification settings
- `api/auth-config.py`
  returns the public Supabase auth config needed by the browser sign-in flow
- `api/_lib.py`
  shared source fetchers, fit scoring, normalization, and Supabase REST helpers
- `api/fairpicture_position.py`
  scores country experience ("can we deliver there")
- `api/client_warmth.py`
  scores the issuing organisation ("do they already know us")

Database:
- `supabase/schema.sql`

## Client Warmth

Every opportunity is scored against Fairpicture's client roster so warm leads stay at the top
of the desk instead of being lost in the volume of cold tenders. The `Client` column sorts and
filters on four tiers:

| Tier | Badge | Meaning |
| --- | --- | --- |
| Client | `Client · N` | The issuing organisation is an existing client, with N projects on record |
| Sister org | `Sister org` | A national chapter or sibling of a client (Caritas Österreich → Caritas Germany) |
| Network | `Network` | A co-member of an umbrella where Fairpicture already works (DEC, ACT Alliance) |
| New | `—` | No known relationship |

Matching runs on the organisation name (accents, legal suffixes such as `e.V.` and `gGmbH`, and
longer legal names are normalized away) and on the website domain label, so a roster entry of
`naturland.org` still matches a tender published on `naturland.de`.

Warmth is computed at serialization time in `serialize_opportunity_row`, so it needs no schema
migration and no re-sync — changing the roster changes the scores on the next page load.

### Refreshing the roster

`RAW_CLIENT_ROSTER` in [api/client_warmth.py](/Users/hazem/Fairpicture/mvps/vc-opportunities-finder/api/client_warmth.py)
is a static export of `organisation_name, website, project_count`. To refresh it, re-run the
client export query and replace the list. Test and demo rows are dropped explicitly through
`EXCLUDED_ROSTER_NAMES` — extend that set rather than deleting rows, so it stays obvious what
was filtered and why.

Family and umbrella membership live in `ORG_FAMILIES` and `NETWORKS` and are maintained by hand.
A tier only fires when Fairpicture actually has a client in that family or network.

## Manual Supabase Setup

You need to do these steps once in your Supabase project.

### 1. Create the project

Create a Supabase project if you do not already have one.

### 2. Run the schema SQL

In Supabase:

1. Open `SQL Editor`
2. Paste the contents of [supabase/schema.sql](/Users/hazem/Fairpicture/mvps/vc-opportunities-finder/supabase/schema.sql)
3. Run it

This creates:
- `opportunities`
- `sync_runs`
- `notification_settings`
- indexes
- the `updated_at` trigger
- RLS policies restricted to the service role

### 3. Copy the project credentials

In Supabase:

1. Open `Project Settings`
2. Open `API`
3. Copy:
   - `Project URL`
   - `service_role` key

You need these exact environment variables:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `TEAM_ALLOWED_EMAILS` (optional but recommended)
- `ADMIN_EMAIL` (optional fallback admin login)
- `ADMIN_PASSWORD_HASH` (optional fallback admin login)
- `ADMIN_SESSION_SECRET` (optional but recommended when admin login is enabled)
- `POSTMARK_SERVER_TOKEN`
- `POSTMARK_FROM_EMAIL`
- `POSTMARK_FROM_NAME` (optional)
- `SOURCE_HEARTBEAT_URL` (optional, see Source Health Monitoring)

### 4. Add env vars in Vercel

From the project directory:

```bash
vercel env add SUPABASE_URL
vercel env add SUPABASE_ANON_KEY
vercel env add SUPABASE_SERVICE_ROLE_KEY
vercel env add TEAM_ALLOWED_EMAILS
vercel env add ADMIN_EMAIL
vercel env add ADMIN_PASSWORD_HASH
vercel env add ADMIN_SESSION_SECRET
vercel env add POSTMARK_SERVER_TOKEN
vercel env add POSTMARK_FROM_EMAIL
vercel env add POSTMARK_FROM_NAME
```

Then redeploy:

```bash
vercel --prod --yes
```

### 5. Optional local env

For local testing with `vercel dev`, create `.env.local` from [.env.example](/Users/hazem/Fairpicture/mvps/vc-opportunities-finder/.env.example).

## Local Run

To mirror production behavior locally, use:

```bash
vercel dev
```

Then open the local URL Vercel prints.

If you only run a static file server, the frontend will load but the API routes will not behave like production.

## Important Notes

- The ReliefWeb app name is fixed in the backend as:
  `fairpicture-tenderbot2026-20srf`
- ReliefWeb requires an approved app name:
  [https://apidoc.reliefweb.int/parameters#appname](https://apidoc.reliefweb.int/parameters#appname)
- Cross-source deduplication now merges matching tenders into one row and keeps the matched source list on that record
- access should be limited to invited users in Supabase Auth
- disable public signup in Supabase: `Authentication` → `Providers` → `Email` and turn off self sign-up
- create users from Supabase via `Authentication` → `Users` → `Invite user`
- if `TEAM_ALLOWED_EMAILS` is set, the backend rejects any signed-in email not on that allowlist
- admin fallback auth also exists for bootstrap/recovery; if used, configure `ADMIN_SESSION_SECRET` and either seed `public.admin_users` or set `ADMIN_EMAIL` plus `ADMIN_PASSWORD_HASH`
- The refresh endpoint includes a simple running-sync guard to reduce duplicate source fetches
- Notification recipient emails, sender override, and expiry lead time are configurable in the app UI and stored in Supabase
- New-tender emails send once per tender; about-to-expire alerts send once per tender for the currently configured lead time; expired-tender alerts send once when an item moves to expired
- The sender email must be valid for your Postmark server or sender signature

## Source Health Monitoring

A source that breaks used to say nothing: UNGM returned no results for 51 days and
every run still reported `completed`. Each run now records, per source, how the
source behaved rather than only what it yielded.

`source_runs` stores one row per source per run: timings, HTTP status, rows fetched,
rows parsed, rows matched, rows new, rows with a missing deadline, rows with an
unusable title, and the error if there was one.

Quality is judged over the rows a run *kept*, not the rows it parsed: the counts
come from the items the fetcher returned, and UNDP parses 535 notices to keep 6, so
a threshold applied to the parsed count could never be reached.

The distinction between **parsed** and **matched** is the point. Welthungerhilfe
parses 23 tenders every run and matches none, because it publishes food and
logistics work. Parsed rows say the page still works; matched rows say whether it
had anything for us. A rule reading only matched rows calls that source dead daily.

### The rules

| Rule | Fires when |
| --- | --- |
| `hard_fail` | The fetch raised, returned a non-2xx status, or parsed 0 rows while its own last 30 runs median above 0 |
| `quiet_source` | No new rows for more than N consecutive business days |
| `quality_drop` | More than 30% of the rows a run kept have no deadline or an unusable title, and it kept at least 5 |
| `control_probe` | UNGM only — the search page reports its own result count and fewer than half were parsed |

N is derived per source from its own rhythm: the 95th-percentile business-day gap
between the days it produced new rows, with a floor. A source with no history is
`unknown`, never `healthy`.

The floor (`source_quiet_floor_days`, default 2) and per-source overrides
(`source_quiet_overrides`, e.g. `{"Welthungerhilfe": 45}`) live in
`notification_settings`.

Some sources match nothing in a normal month by design — they publish a handful of
tenders at a time and only occasionally a media one. Those carry a committed
default in `SPARSE_SOURCE_QUIET_BUSINESS_DAYS` (Welthungerhilfe, Plan International
and CAF, at 90 business days) so they do not open an incident within days of being
switched on. A value in `source_quiet_overrides` still wins over it. For these the
`hard_fail` rule does the real watching, which is why both new fetchers carry a
probe: without one a run row reports `rowsParsed` as the matched count, and a page
that silently stopped yielding tenders would look the same as a quiet week.

### Retiring a source

`MANAGED_SOURCES` is the read filter on the opportunities table, so a source
removed from it loses every tender it ever contributed from the desk, the emails
and Insights. To stop fetching a source without losing its history, add it to
`RETIRED_SOURCES`: it stays readable and filterable, drops out of `ACTIVE_SOURCES`,
and is no longer fetched, judged or shown in the health strip.

ICIMOD is retired. It contributed one tender in eight months, found 26 March 2026
and declined, for 9.8 seconds of every run.

### Plan International and CAF

Both were written when the tool was first built and never switched on, so for
months nothing fetched them. They are live as of 6 November 2026, and both are
expected to match nothing most weeks:

- **Plan International** lists two or three tenders at a time; the current ones are
  generators and a data review. 0.5s per run.
- **CAF** is the African football confederation, and it does tender the work we do:
  AFCON 2027 photography, audiovisual and creative content production, WAFCON
  filming. It keeps only currently-open tenders on the site and deletes the rest, so
  all three of those are already 404 and the one tender open today is health
  insurance. The value is forward-looking: catching the next one on the day it is
  posted. 2.5s per run, and its index links to tenders it has already deleted, which
  is why a 404 on a detail page is skipped rather than failing the source.

### Alerts

One email when an incident opens, a reminder every 3 days while it stays open, one
when it clears. Never one per failed run. Each carries the source, the rule, the
last good run, the rows trend over the last 7 runs, and a link to the desk.

### Dead man's switch

The alerting above cannot fire if the daily job never runs. Set `SOURCE_HEARTBEAT_URL`
to a URL from any uptime monitor that watches for an inbound ping — Healthchecks.io,
Cronitor, Better Stack and others all expose one. The job calls it once, at the end of
a run that completed. A run that failed outright sends nothing, so the monitor notices;
a run where one source failed still pings, because that is what the incident alerts
are for and a flaky source must not look like a job that never ran.

```bash
vercel env add SOURCE_HEARTBEAT_URL
```

Then, in the monitor:

1. Create a check with a period of 1 day and a grace window that ends at 07:00
   Africa/Cairo — the Vercel cron runs at 06:00 UTC, which gives the job an hour.
2. Set the alert channel to the address that should hear about it.
3. Copy the check's ping URL into `SOURCE_HEARTBEAT_URL` and redeploy.

No vendor is hard-coded: the job issues a plain GET to whatever URL is configured,
and does nothing at all when the variable is unset.
