-- The synthetic holdings the extract tests run against (SQL Server; batches split
-- on GO lines). Rows are invented; each expected CSV under tests/cases/ is these
-- rows passed through the case's order by hand.
CREATE SCHEMA P1105;
GO
-- Tables named by the conformance order goldens (conformance/cases/order).
-- Range: the row-filter goldens' table, filtered on AR (year, int). 2017 lies
-- before the held edition (an open lower bound keeps it); NULL matches no range.
CREATE TABLE dbo.Range (Observed int, AR int);
INSERT INTO dbo.Range VALUES (10, 2017), (11, 2018), (12, 2019), (13, 2020), (14, NULL);
-- partitions: one table per shard.
CREATE TABLE dbo.ShardA (Observed int);
INSERT INTO dbo.ShardA VALUES (1), (2);
CREATE TABLE dbo.ShardB (Observed int);
INSERT INTO dbo.ShardB VALUES (3);
-- projections: Narrow is a projection of Wide; ids are bigint person keys.
CREATE TABLE dbo.Wide (Id bigint, Field int, Region int);
INSERT INTO dbo.Wide VALUES (199001011234, 1, 1), (199002021234, 2, 2), (199003031234, 3, 3);
CREATE TABLE dbo.Narrow (Id bigint, Extra nvarchar(50), Child bigint, FieldB int);
INSERT INTO dbo.Narrow VALUES
  (199001011234, N'say "hi", then go', 201001011234, 1),
  (199002021234, N'', NULL, 2),
  (199003031234, NULL, 201203031234, 3);
-- column-windows: one table serving two variants, and a later table.
CREATE TABLE dbo.Combined (Id bigint, OldField int, NewField int, Child bigint);
INSERT INTO dbo.Combined VALUES (1, 10, NULL, NULL), (2, NULL, 20, NULL);
CREATE TABLE dbo.Later (Id bigint, OldField int);
INSERT INTO dbo.Later VALUES (3, 30);
GO
-- Runner cases (tests/cases/*/order.json): one period column per encoding and
-- delivery type, a schema-P1105 table with non-ASCII names, and the refusals.
-- Monthly also carries code-page text (varchar) and an 18-digit decimal id.
CREATE TABLE dbo.Monthly (Payment int, Period int, Place varchar(20), Ref decimal(18, 0));
INSERT INTO dbo.Monthly VALUES
  (1, 201901, 'Lund', 100000000000000001),
  (2, 201902, N'Västerås', 123456789012345678),
  (3, 201903, NULL, 999999999999999999),
  (4, 201904, 'Kiruna', 100000000000000004);
CREATE TABLE dbo.Yearly (Value int, Ar varchar(10));
INSERT INTO dbo.Yearly VALUES (1, '2018'), (2, '2019'), (3, 'okänt'), (4, NULL), (5, '2020');
CREATE TABLE dbo.Stamped (Value int, Seen datetime2);
INSERT INTO dbo.Stamped VALUES
  (1, '2018-12-31T10:00:00'), (2, '2019-01-01T23:59:59'), (3, '2019-01-02T00:00:00');
CREATE TABLE dbo.Twin (Value int);
GO
CREATE TABLE P1105.Twin (Value int);
CREATE TABLE P1105.[Vårdtillfälle] ([Värde] nvarchar(50), [Datum] float);
INSERT INTO P1105.[Vårdtillfälle] VALUES
  (N'Göteborg', 20190101), (N'Malmö', 20190102), (N'Umeå', 20190103);
GO
-- Typed: one value of each SQL type a .dta or .parquet file maps (a leading-zero
-- code, a day before Stata's 1960 epoch, a 12-digit bigint id, an 18-digit
-- decimal no double holds, a decimal with a scale), a row of NULLs and a row with
-- an empty string. Long: a column name over Stata's 32 characters.
CREATE TABLE dbo.Typed (
  LopNr bigint, Kod char(4), Datum date, Ref decimal(18, 0), Belopp decimal(9, 2),
  Antal int, Stamp datetime2, Flag bit, Namn nvarchar(50)
);
INSERT INTO dbo.Typed VALUES
  (199001011234, '0180', '1959-12-31', 123456789012345678, 1234567.89, -5,
   '2019-01-01T10:00:00', 1, N'Västerås'),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (201001011234, '0001', '2020-02-29', 1, -0.5, 7, '2020-02-29T23:59:59', 0, N'');
CREATE TABLE dbo.Long (LopNrForSammanstalldaForetagsuppgifter bigint);
GO
-- Fails when read, not when resolved: the extraction-failure case.
CREATE VIEW dbo.Broken AS SELECT CAST(1 AS int) / CAST(0 AS int) AS X;
GO
