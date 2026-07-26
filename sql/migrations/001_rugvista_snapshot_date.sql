alter table rugvista_variant_snapshot add column if not exists snapshot_date date;

update rugvista_variant_snapshot
   set snapshot_date = (captured_at at time zone 'Europe/Stockholm')::date
 where snapshot_date is null;

alter table rugvista_variant_snapshot alter column snapshot_date set not null;

create unique index if not exists rugvista_variant_snapshot_day_uk
  on rugvista_variant_snapshot (snapshot_date, product_id);
