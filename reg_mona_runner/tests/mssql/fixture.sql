-- The synthetic holdings the extract tests run against (SQL Server; batches split
-- on GO lines). Rows are invented; each expected file under tests/cases/ is these
-- rows passed through the case's order by hand.
CREATE SCHEMA P1105;
GO
-- Tables named by the materialize goldens (conformance/cases/materialize), typed
-- exactly as the steward fixture's storage export says
-- (conformance/cases/holdings/steward/storage.csv with the slot family's
-- holdings/slot-family/storage.csv and materialize/fixture/long-column-storage.csv),
-- so the runner's storage check proves the two fixtures agree.
-- Pool: the pooled 2017..2020 delivery, filtered on Year (year, int). 2016 and
-- 2021 lie outside the delivery and 2018 in the disjoint golden's gap, so every
-- filter cuts on both sides; NULL matches no range and is kept only unfiltered.
CREATE TABLE dbo.Pool (
  Received_Id int, Old int, New int, Late nvarchar(10), Unknown varchar(10),
  Slot2 int, Code int, Year int, Position10 int, Position1 int
);
INSERT INTO dbo.Pool VALUES
  (1, 11, 21, N'L1', 'U1', 41, 51, 2016, 61, 71),
  (2, 12, 22, N'L2', 'U2', 42, 52, 2017, 62, 72),
  (3, 13, 23, N'L3', 'U3', 43, 53, 2018, 63, 73),
  (4, 14, 24, N'L4', 'U4', 44, 54, 2019, 64, 74),
  (5, 15, 25, N'L5', 'U5', 45, 55, 2020, 65, 75),
  (6, 16, 26, N'L6', 'U6', 46, 56, 2021, 66, 76),
  (7, 17, 27, NULL, 'U7', 47, 57, NULL, 67, 77);
-- Partition members; Part B also delivers a header that is no Stata name.
CREATE TABLE dbo.[Part A] (Id int, New int);
INSERT INTO dbo.[Part A] VALUES (1, 101), (2, 102);
CREATE TABLE dbo.[Part B] (
  Id int, New int, [Relative identifier as received in the delivery] int
);
INSERT INTO dbo.[Part B] VALUES (3, 103, 903), (4, NULL, 904);
-- Child is a projection of Parent carrying its Id.
CREATE TABLE dbo.Parent (Id int, Amount int, Status int);
INSERT INTO dbo.Parent VALUES (1, 500, 1), (2, 600, 2);
CREATE TABLE dbo.Child (Id int, ChildId int, Amount int, Status int);
INSERT INTO dbo.Child VALUES (1, 11, 700, 1), (1, 12, 800, 2);
GO
-- Runner cases (tests/cases/*/order.json): one period column per encoding and
-- delivery type, a schema-P1105 table with non-ASCII names, and the refusals.
-- Monthly also carries code-page text (varchar) and an 18-digit decimal id; it
-- exists in dbo only, so missing-table's P1105.Monthly is not found.
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
GO
CREATE TABLE P1105.[Vårdtillfälle] ([Värde] nvarchar(50), [Datum] float);
INSERT INTO P1105.[Vårdtillfälle] VALUES
  (N'Göteborg', 20190101), (N'Malmö', 20190102), (N'Umeå', 20190103);
GO
-- Typed: one value of each SQL type a .dta or .parquet file maps (a leading-zero
-- code, a day before Stata's 1960 epoch, a 12-digit bigint id, an 18-digit
-- decimal no double holds, a decimal with a scale), a row of NULLs and a row with
-- an empty string. Inexact: a bigint id 2^53 + 1, which no double holds.
CREATE TABLE dbo.Typed (
  LopNr bigint, Kod char(4), Datum date, Ref decimal(18, 0), Belopp decimal(9, 2),
  Antal int, Stamp datetime2, Flag bit, Namn nvarchar(50)
);
INSERT INTO dbo.Typed VALUES
  (199001011234, '0180', '1959-12-31', 123456789012345678, 1234567.89, -5,
   '2019-01-01T10:00:00', 1, N'Västerås'),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (201001011234, '0001', '2020-02-29', 1, -0.5, 7, '2020-02-29T23:59:59', 0, N'');
CREATE TABLE dbo.Inexact (LopNr bigint);
INSERT INTO dbo.Inexact VALUES (9007199254740993);
GO
-- Fails when read, not when resolved: the extraction-failure case.
CREATE VIEW dbo.Broken AS SELECT CAST(1 AS int) / CAST(0 AS int) AS X;
GO
