DROP TRIGGER IF EXISTS outbox_events_guard ON outbox_events;
DROP FUNCTION IF EXISTS outbox_events_guard();
DROP TABLE IF EXISTS outbox_events;
