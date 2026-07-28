# Fairpicture Opportunity Finder

Internal Fairpicture tool for discovering media-related opportunities from:

- ReliefWeb
- UNDP Procurement
- UNGM
- ICIMOD
- Welthungerhilfe

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
