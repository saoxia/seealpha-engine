-- One-time destructive reset for a fresh single-installation SeeAlpha schema.
-- Run only while the Engine, Pipeline and Strategy processes are stopped.
-- This intentionally removes all SeeAlpha-owned application tables and data.
-- It does NOT erase NautilusTrader's public schema or the PostgreSQL cluster.
DROP SCHEMA IF EXISTS seealpha CASCADE;
