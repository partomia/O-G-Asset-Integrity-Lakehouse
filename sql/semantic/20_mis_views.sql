-- MIS views: aggregates of the certified KPI views only, never a re-derived formula, so every
-- figure here reconciles to its KPI view (scripts/run_semantic.py check).

DROP VIEW IF EXISTS rsingh_ogx_semantic.mis_risk_by_facility;

CREATE VIEW rsingh_ogx_semantic.mis_risk_by_facility
COMMENT 'MIS: integrity risk exposure, high-risk and abstained assets by facility and business date (kpi_integrity_risk_exposure)'
AS
SELECT m.*, CASE WHEN m.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM (
SELECT business_date, facility_id,
       COUNT(*)                                              AS assets,
       SUM(exposure)                                         AS exposure,
       SUM(CASE WHEN risk_band = 'HIGH' THEN 1 ELSE 0 END)   AS high_risk_assets,
       SUM(CASE WHEN risk_band = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_assets,
       SUM(is_abstained)                                     AS abstained_assets
FROM rsingh_ogx_semantic.kpi_integrity_risk_exposure
GROUP BY business_date, facility_id
) m
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_ogx_semantic.kpi_integrity_risk_exposure) l;

DROP VIEW IF EXISTS rsingh_ogx_semantic.mis_risk_worklist;

CREATE VIEW rsingh_ogx_semantic.mis_risk_worklist
COMMENT 'MIS: the risk-ranked asset worklist per business date (kpi_integrity_risk_exposure)'
AS
SELECT m.*, CASE WHEN m.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM (
SELECT business_date, asset_id, tag, asset_class, facility_id, criticality, wall_loss_pct,
       breach_windows_3d, corrective_wo_30d, days_overdue, rule_score, risk_band, p_event_30d,
       exposure, is_abstained, abstain_reason,
       ROW_NUMBER() OVER (PARTITION BY business_date ORDER BY exposure DESC, rule_score DESC, asset_id) AS risk_rank
FROM rsingh_ogx_semantic.kpi_integrity_risk_exposure
) m
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_ogx_semantic.kpi_integrity_risk_exposure) l;

DROP VIEW IF EXISTS rsingh_ogx_semantic.mis_coverage_by_format;

CREATE VIEW rsingh_ogx_semantic.mis_coverage_by_format
COMMENT 'MIS: unstructured coverage by format and business date (kpi_unstructured_coverage)'
AS
SELECT m.*, CASE WHEN m.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM (
SELECT business_date, object_format,
       SUM(is_received)                                     AS received,
       SUM(is_extracted)                                    AS extracted,
       SUM(is_covered)                                      AS covered,
       CAST(SUM(is_covered) AS DOUBLE) / SUM(is_received)   AS coverage
FROM rsingh_ogx_semantic.kpi_unstructured_coverage
GROUP BY business_date, object_format
) m
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_ogx_semantic.kpi_unstructured_coverage) l;

DROP VIEW IF EXISTS rsingh_ogx_semantic.mis_time_to_review;

CREATE VIEW rsingh_ogx_semantic.mis_time_to_review
COMMENT 'MIS: severe-defect hours to review, risk order against calendar order (kpi_severe_time_to_review)'
AS
SELECT m.*, CASE WHEN m.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM (
SELECT business_date, worklist_order,
       COUNT(*)              AS severe_defects,
       AVG(hours_to_review)  AS avg_hours,
       MIN(hours_to_review)  AS min_hours,
       MAX(hours_to_review)  AS max_hours
FROM rsingh_ogx_semantic.kpi_severe_time_to_review
GROUP BY business_date, worklist_order
) m
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_ogx_semantic.kpi_severe_time_to_review) l;
