CREATE OR REPLACE FUNCTION notify_mqtt()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'mqtt',
    json_build_object(
      'operation', TG_OP,
      'record', row_to_json(NEW)
    )::text
  );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION notify_mqtt_delete()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'mqtt',
    json_build_object(
      'operation', TG_OP,
      'record', row_to_json(OLD)
    )::text
  );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop table

-- DROP TABLE public.table_with_pk;

CREATE TABLE public.table_with_pk (
	a serial NOT NULL,
	b varchar(30) NULL,
	c timestamp NOT NULL DEFAULT now(),
	CONSTRAINT table_with_pk_pkey PRIMARY KEY (a, c)
);

-- Table Triggers

-- DROP TRIGGER table_with_pk_changed ON public.table_with_pk;

create trigger table_with_pk_changed after
insert
	or
update
	on
	public.table_with_pk for each row execute function notify_mqtt();
-- DROP TRIGGER table_with_pk_deleted ON public.table_with_pk;

 create trigger table_with_pk_deleted after
delete
	on
	public.table_with_pk for each row execute function notify_mqtt_delete();

-- DROP TRIGGER table_with_pk_truncated ON public.table_with_pk;
 create trigger table_with_pk_truncated after
truncate
	on
	public.table_with_pk for each statement execute function notify_mqtt();