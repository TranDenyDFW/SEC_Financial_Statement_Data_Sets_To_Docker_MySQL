USE secdb;

DROP TABLE IF EXISTS tmp_tbl1;

DROP TABLE IF EXISTS tmp_tbl2;

DROP TABLE IF EXISTS tmp_tbl3;

CREATE TABLE tmp_tbl1 AS
  SELECT name,
         Concat(' [', sec_sic.industry, ']') indus,
         adsh,
         fye,
         form,
         period,
         fy,
         fp,
         cik
  FROM   sub2023
         INNER JOIN sec_sic
                 ON sec_sic.sic = sub2023.sic
  WHERE  fp = 'FY'
         AND cik = 21344
         AND form = '10-K'
         AND Instr(period, fye);

CREATE TABLE tmp_tbl2 AS
  SELECT DISTINCT adsh,
                  tag,
                  plabel,
                  version,
                  Cast(line AS signed) AS line
  FROM   pre2023
  WHERE  stmt = 'BS'
         AND adsh = (SELECT adsh
                     FROM   tmp_tbl1
                     LIMIT  1)
         AND inpth != 1;

CREATE TABLE tmp_tbl3 AS
  SELECT Concat(num2023.adsh, tag, version)                       AS numpre_ref,
         ddate,
         Cast(qtrs AS signed)                                     AS qtrs,
         uom,
         value,
         (SELECT IF(footnote = '-1', NULL, footnote) AS footnote) AS footnote
  FROM   num2023
         JOIN tmp_tbl1
           ON ( tmp_tbl1.period - ddate ) % 10000 = 0
  WHERE  num2023.adsh = tmp_tbl1.adsh;

SET @max_year = (SELECT max(substring(ddate, 1, 4)) FROM tmp_tbl3);

SET @second_max_year = (SELECT max(substring(ddate, 1, 4)) FROM tmp_tbl3 WHERE
substring(ddate, 1, 4) <> @max_year);

SET @company_name = (SELECT name FROM tmp_tbl1 LIMIT 1);

SET @company_industry = (SELECT indus FROM tmp_tbl1 LIMIT 1);

SELECT ' '               AS line,
       @company_name     AS tag,
       @company_industry AS plabel,
       ' '               AS uom,
       ' '               AS fy_t,
       ' '               AS 'fy_t-1',
       ' '               AS footnote_t,
       ' '               AS 'footnote_t-1'
UNION
SELECT tmp_tbl2.line,
       tag,
       plabel,
       uom,
       Max(CASE
             WHEN Substring(ddate, 1, 4) = @max_year THEN Format(value, 0)
           end) AS fy_t,
       Max(CASE
             WHEN Substring(ddate, 1, 4) = @second_max_year THEN
             Format(value, 0)
           end) AS 'fy_t-1',
       Max(CASE
             WHEN Substring(ddate, 1, 4) = @max_year THEN footnote
           end) AS footnote_t,
       Max(CASE
             WHEN Substring(ddate, 1, 4) = @second_max_year THEN footnote
           end) AS 'footnote_t-1'
FROM   tmp_tbl2
       LEFT JOIN tmp_tbl3
              ON Concat(tmp_tbl2.adsh, tmp_tbl2.tag, tmp_tbl2.version) =
                 tmp_tbl3.numpre_ref
GROUP  BY tmp_tbl2.line,
          uom,
          tag,
          plabel
ORDER  BY line;

DROP TABLE IF EXISTS tmp_tbl1;

DROP TABLE IF EXISTS tmp_tbl2;

DROP TABLE IF EXISTS tmp_tbl3; 