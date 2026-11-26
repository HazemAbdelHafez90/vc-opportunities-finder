-- Run in the Supabase SQL editor before deploying feedback.
create table if not exists public.opportunity_feedback (
  id uuid primary key default gen_random_uuid(),
  opportunity_id uuid not null references public.opportunities(id) on delete cascade,
  reporter_email text not null,
  comment text not null default '',
  created_at timestamptz not null default now()
);
create index if not exists opportunity_feedback_created_at_idx on public.opportunity_feedback (created_at desc);
alter table public.opportunity_feedback enable row level security;
