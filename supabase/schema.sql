create extension if not exists pgcrypto;

create table if not exists public.opportunities (
  id uuid primary key default gen_random_uuid(),
  source text not null,
  source_item_id text not null,
  title text not null,
  organization text,
  countries jsonb not null default '[]'::jsonb,
  deadline timestamptz null,
  type text,
  link text,
  fit_score integer not null default 0,
  fit_label text not null default 'Low fit',
  fit_reasons jsonb not null default '[]'::jsonb,
  action_status text null check (action_status in ('applied', 'not_interested', 'not_relevant')),
  action_notes text null,
  action_taken_at timestamptz null,
  status text not null default 'open' check (status in ('open', 'expired')),
  new_notification_sent_at timestamptz null,
  expiry_notification_sent_at timestamptz null,
  expiry_notification_sent_days integer null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_synced_at timestamptz not null default now(),
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source, source_item_id)
);

alter table public.opportunities
  add column if not exists new_notification_sent_at timestamptz null,
  add column if not exists expiry_notification_sent_at timestamptz null,
  add column if not exists expiry_notification_sent_days integer null,
  add column if not exists expired_notification_sent_at timestamptz null;

create table if not exists public.sync_runs (
  id uuid primary key default gen_random_uuid(),
  started_at timestamptz not null default now(),
  finished_at timestamptz null,
  status text not null check (status in ('running', 'completed', 'failed')),
  triggered_by text not null default 'manual',
  sources jsonb not null default '[]'::jsonb,
  new_count integer not null default 0,
  updated_count integer not null default 0,
  error_log text null
);

create table if not exists public.sync_run_sources (
  id uuid primary key default gen_random_uuid(),
  sync_run_id uuid not null references public.sync_runs(id) on delete cascade,
  source text not null,
  status text not null check (status in ('completed', 'failed', 'skipped')),
  item_count integer not null default 0,
  error_message text null,
  finished_at timestamptz not null default now()
);

create table if not exists public.notification_settings (
  id boolean primary key default true check (id = true),
  enabled boolean not null default true,
  new_tender_enabled boolean not null default true,
  expiry_alert_enabled boolean not null default true,
  recipient_emails jsonb not null default '[]'::jsonb,
  sender_name text null,
  sender_email text null,
  expiry_alert_days integer not null default 2 check (expiry_alert_days >= 0 and expiry_alert_days <= 30),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.notification_settings
  add column if not exists enabled boolean not null default true,
  add column if not exists new_tender_enabled boolean not null default true,
  add column if not exists expiry_alert_enabled boolean not null default true,
  add column if not exists recipient_emails jsonb not null default '[]'::jsonb,
  add column if not exists sender_name text null,
  add column if not exists sender_email text null,
  add column if not exists expiry_alert_days integer not null default 2,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

alter table public.notification_settings
  drop constraint if exists notification_settings_expiry_alert_days_check;

alter table public.notification_settings
  add constraint notification_settings_expiry_alert_days_check
  check (expiry_alert_days >= 0 and expiry_alert_days <= 30);

create index if not exists opportunities_status_idx on public.opportunities (status);
create index if not exists opportunities_action_status_idx on public.opportunities (action_status);
create index if not exists opportunities_fit_score_idx on public.opportunities (fit_score desc);
create index if not exists opportunities_deadline_idx on public.opportunities (deadline asc);
create index if not exists opportunities_new_notification_sent_idx on public.opportunities (new_notification_sent_at);
create index if not exists opportunities_expiry_notification_sent_idx on public.opportunities (expiry_notification_sent_at);
create index if not exists sync_runs_started_at_idx on public.sync_runs (started_at desc);
create index if not exists sync_run_sources_sync_run_id_idx on public.sync_run_sources (sync_run_id, finished_at desc);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists opportunities_set_updated_at on public.opportunities;
create trigger opportunities_set_updated_at
before update on public.opportunities
for each row
execute function public.set_updated_at();

drop trigger if exists notification_settings_set_updated_at on public.notification_settings;
create trigger notification_settings_set_updated_at
before update on public.notification_settings
for each row
execute function public.set_updated_at();

alter table public.opportunities enable row level security;
alter table public.sync_runs enable row level security;
alter table public.sync_run_sources enable row level security;
alter table public.notification_settings enable row level security;

drop policy if exists "service role only opportunities" on public.opportunities;
create policy "service role only opportunities"
on public.opportunities
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists "service role only sync_runs" on public.sync_runs;
create policy "service role only sync_runs"
on public.sync_runs
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists "service role only sync_run_sources" on public.sync_run_sources;
create policy "service role only sync_run_sources"
on public.sync_run_sources
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

drop policy if exists "service role only notification_settings" on public.notification_settings;
create policy "service role only notification_settings"
on public.notification_settings
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

insert into public.notification_settings (id)
values (true)
on conflict (id) do nothing;
