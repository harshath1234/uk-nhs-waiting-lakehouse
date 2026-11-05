-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Audit a revised month with Delta time travel
-- MAGIC Run after a revised publication has been merged (e.g. a month NHS England re-issued).
-- MAGIC Set `version_before` from `DESCRIBE HISTORY`.

-- COMMAND ----------
DESCRIBE HISTORY nhs.silver.rtt_pathway;

-- COMMAND ----------
-- Incomplete pathways per publication for one month, before vs after the revision
SELECT 'before' AS state, _published, SUM(total_all) AS incomplete_pathways
FROM nhs.silver.rtt_pathway VERSION AS OF ${version_before}
WHERE part_type = 'Part_2' AND period_month = DATE'${period}'
GROUP BY _published
UNION ALL
SELECT 'after', _published, SUM(total_all)
FROM nhs.silver.rtt_pathway
WHERE part_type = 'Part_2' AND period_month = DATE'${period}'
GROUP BY _published;

-- COMMAND ----------
-- Providers whose figures changed in the revision
SELECT COALESCE(a.provider_code, b.provider_code) AS provider_code,
       b.total AS before_total, a.total AS after_total, a.total - b.total AS change
FROM (SELECT provider_code, SUM(total_all) total FROM nhs.silver.rtt_pathway
      WHERE part_type = 'Part_2' AND period_month = DATE'${period}' GROUP BY 1) a
FULL OUTER JOIN
     (SELECT provider_code, SUM(total_all) total FROM nhs.silver.rtt_pathway VERSION AS OF ${version_before}
      WHERE part_type = 'Part_2' AND period_month = DATE'${period}' GROUP BY 1) b
  ON a.provider_code = b.provider_code
WHERE a.total IS DISTINCT FROM b.total
ORDER BY ABS(COALESCE(a.total, 0) - COALESCE(b.total, 0)) DESC;
