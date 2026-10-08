-- A second window starting one day into the first browse delivery's first window
-- overlaps it. Fails if the disjointness check stops comparing a window's start
-- with the previous window's end.
INSERT INTO delivery_window (browse_delivery_id, valid_from, valid_to)
SELECT browse_delivery_id, date(valid_from, '+1 day'), valid_to FROM delivery_window
WHERE browse_delivery_id = (SELECT MIN(browse_delivery_id) FROM delivery_window)
ORDER BY valid_from LIMIT 1;
