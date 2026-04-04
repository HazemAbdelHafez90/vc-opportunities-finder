# Fairpicture Opportunity Finder

Internal Fairpicture tool for discovering media-related opportunities from:

- ReliefWeb
- UNDP Procurement
- UNGM
- ICIMOD

The app now uses Supabase as its cache and read layer:

- page load reads cached open opportunities from Supabase
- the UI shows `Last synced X ago`
- clicking `Refresh Results` fetches live sources, recalculates fit scores, upserts into Supabase, and reloads the table
- expired items stay in the database with `status = expired` and are hidden from the main UI

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
- `api/_lib.py`
  shared source fetchers, fit scoring, normalization, and Supabase REST helpers

Database:
- `supabase/schema.sql`

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
- `SUPABASE_SERVICE_ROLE_KEY`
- `POSTMARK_SERVER_TOKEN`
- `POSTMARK_FROM_EMAIL`
- `POSTMARK_FROM_NAME` (optional)

### 4. Add env vars in Vercel

From the project directory:

```bash
vercel env add SUPABASE_URL
vercel env add SUPABASE_SERVICE_ROLE_KEY
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
- Cross-source deduplication is not implemented yet
- Anyone can trigger refresh
- The refresh endpoint includes a simple running-sync guard to reduce duplicate source fetches
- Notification recipient emails, sender override, and expiry lead time are configurable in the app UI and stored in Supabase
- New-tender emails send once per tender; about-to-expire alerts send once per tender for the currently configured lead time; expired-tender alerts send once when an item moves to expired
- The sender email must be valid for your Postmark server or sender signature
