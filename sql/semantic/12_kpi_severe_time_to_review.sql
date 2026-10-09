-- Certified KPI: severe-defect time to review (ref.kpi_definition kpi_code = 'TTR').
-- Grain: severe defect x worklist order x business date. An engineer starts the next morning
-- (review_shift_start_hour) and spends review_hours_per_inspection on each of the day's
-- inspections, either in risk order (gold.fact_asset_risk_daily rule score, highest first) or
-- in calendar order (oldest capture first). A defect is severe at wall loss >= severe_wall_loss_pct.
-- Consumers aggregate hours by worklist_order: AVG, MAX, or the engine's median.

DROP VIEW IF EXISTS rsingh_ogx_semantic.kpi_severe_time_to_review;

CREATE VIEW rsingh_ogx_semantic.kpi_severe_time_to_review
COMMENT 'Certified KPI TTR v1.0: severe defect x worklist order (risk, calendar) x business date, hours to review'
AS
WITH p AS (
    SELECT business_date,
           MAX(CASE WHEN parameter = 'review_hours_per_inspection' THEN value END) AS hours_each,
           MAX(CASE WHEN parameter = 'review_shift_start_hour' THEN value END)     AS start_hour,
           MAX(CASE WHEN parameter = 'severe_wall_loss_pct' THEN value END)        AS severe_pct
    FROM rsingh_ogx_ref.kpi_parameter GROUP BY business_date
),
i AS (
    SELECT f.business_date, f.inspection_id, f.doc_id, f.asset_id, f.equipment AS tag, f.`method`,
           f.wall_loss_pct, f.inspected_at, COALESCE(r.rule_score, 0) AS rule_score,
           p.hours_each, p.severe_pct,
           unix_timestamp(CAST(concat(CAST(date_add(f.business_date, 1) AS STRING), ' 00:00:00') AS TIMESTAMP))
               + CAST(p.start_hour * 3600 AS BIGINT) AS shift_start_s
    FROM rsingh_ogx_gold.fact_inspection f
    JOIN p ON p.business_date = f.business_date
    LEFT JOIN rsingh_ogx_gold.fact_asset_risk_daily r
           ON r.asset_id = f.asset_id AND r.business_date = f.business_date
),
q AS (
    SELECT i.*, 'risk' AS worklist_order,
           ROW_NUMBER() OVER (PARTITION BY business_date ORDER BY rule_score DESC, inspected_at, inspection_id) AS pos
    FROM i
    UNION ALL
    SELECT i.*, 'calendar' AS worklist_order,
           ROW_NUMBER() OVER (PARTITION BY business_date ORDER BY inspected_at, inspection_id) AS pos
    FROM i
)
SELECT business_date, worklist_order, inspection_id, doc_id, asset_id, tag, `method`, wall_loss_pct,
       rule_score, inspected_at, pos AS worklist_position,
       CAST((shift_start_s + pos * hours_each * 3600 - unix_timestamp(inspected_at)) / 3600.0 AS DOUBLE)
           AS hours_to_review
FROM q
WHERE wall_loss_pct >= severe_pct;
