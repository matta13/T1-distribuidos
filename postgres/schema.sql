-- schema.sql
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.querys (
  score  integer,
  title  text,
  body   text,
  answer text
);

-- Opcional: índice para búsquedas rápidas
-- CREATE INDEX IF NOT EXISTS idx_querys_title ON public.querys USING gin (to_tsvector('spanish', title));
