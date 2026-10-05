-- A live successor whose curated predecessor slug has no live classification row
-- keeps supersedes_id NULL: the dangling-slug check fails, the projection check
-- does not count it as a missing pointer.
INSERT INTO classification (short_name, name, slug)
VALUES ('SUN-NIVA2000', 'SUN-NIVA2000', 'sun-niva2000');
INSERT INTO classification_replaced_by (predecessor_slug, successor_slug, effective_year, note)
VALUES ('sun1996', 'sun-niva2000', 2000, 'curated:slug_toml');
