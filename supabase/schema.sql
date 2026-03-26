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
  status text not null default 'open' check (status in ('open', 'expired')),
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_synced_at timestamptz not null default now(),
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source, source_item_id)
);

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

create index if not exists opportunities_status_idx on public.opportunities (status);
create index if not exists opportunities_fit_score_idx on public.opportunities (fit_score desc);
create index if not exists opportunities_deadline_idx on public.opportunities (deadline asc);
create index if not exists sync_runs_started_at_idx on public.sync_runs (started_at desc);

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

alter table public.opportunities enable row level security;
alter table public.sync_runs enable row level security;

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
