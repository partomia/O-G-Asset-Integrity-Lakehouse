-- Datasets of the Data Visualization dashboards (dataviz/build_dashboard.py): flat views with
-- labels, 0/1 flags a visual can sum, and is_latest = 1 on the latest business date. No KPI
-- figure is computed here.

DROP VIEW IF EXISTS rsingh_ogx_semantic.dash_recon;

CREATE VIEW rsingh_ogx_semantic.dash_recon
COMMENT 'Dashboard: every reconciliation and KPI consistency check per business date (ref.recon_results)'
AS
SELECT r.business_date, r.batch_id, r.layer,
       CASE r.layer WHEN 'bronze' THEN '1 bronze' WHEN 'stream' THEN '2 stream' WHEN 'silver' THEN '3 silver'
                    WHEN 'gold' THEN '4 gold' WHEN 'semantic' THEN '5 semantic (KPI consumers)'
                    ELSE r.layer END AS layer_label,
       r.entity, r.check_name, r.expected, r.actual, r.difference, r.status, r.detail,
       CASE WHEN r.status = 'MATCHED' THEN 1 ELSE 0 END   AS is_ok,
       CASE WHEN r.status = 'EXPLAINED' THEN 1 ELSE 0 END AS is_explained,
       CASE WHEN r.status = 'LATE' THEN 1 ELSE 0 END      AS is_late,
       CASE WHEN r.status = 'MISMATCH' THEN 1 ELSE 0 END  AS is_mismatch,
       CASE WHEN r.business_date = l.latest_date THEN 1 ELSE 0 END AS is_latest
FROM rsingh_ogx_ref.recon_results r
CROSS JOIN (SELECT MAX(business_date) AS latest_date FROM rsingh_ogx_ref.recon_results) l;

DROP VIEW IF EXISTS rsingh_ogx_semantic.dash_load_audit;

CREATE VIEW rsingh_ogx_semantic.dash_load_audit
COMMENT 'Dashboard: every stage and entity commit or failure per batch (ref.load_audit)'
AS
SELECT a.business_date, a.batch_id, a.pipeline_run, a.stage, a.entity, a.status,
       a.rows_in, a.rows_out, a.rows_rejected, a.snapshot_after, a.started_at, a.ended_at, a.message,
       CASE WHEN a.status = 'FAILED' THEN 1 ELSE 0 END AS is_failed
FROM rsingh_ogx_ref.load_audit a;

DROP VIEW IF EXISTS rsingh_ogx_semantic.dash_asset_master;

CREATE VIEW rsingh_ogx_semantic.dash_asset_master
COMMENT 'Dashboard: how each source name resolved to a golden asset, by rule and business date (asset.asset_xref)'
AS
SELECT x.as_of_date AS business_date, x.src_system, x.src_name, x.asset_id, x.rule, x.score,
       CASE x.rule WHEN 'EXACT_FUNC_LOC' THEN '1 exact functional location'
                   WHEN 'NORMALISED_TAG' THEN '2 normalised tag'
                   WHEN 'GPS_SEGMENT' THEN '3 GPS to pipeline segment' ELSE x.rule END AS rule_label
FROM rsingh_ogx_asset.asset_xref x;

DROP VIEW IF EXISTS rsingh_ogx_semantic.dash_review_queue;

CREATE VIEW rsingh_ogx_semantic.dash_review_queue
COMMENT 'Dashboard: asset-master conflicts and their resolution (asset.review_queue)'
AS
SELECT q.as_of_date AS business_date, q.src_system, q.src_key, q.src_name, q.reason,
       q.suggested_asset_id, q.status, q.detail,
       CASE WHEN q.status = 'OPEN' THEN 1 ELSE 0 END AS is_open
FROM rsingh_ogx_asset.review_queue q;

DROP VIEW IF EXISTS rsingh_ogx_semantic.dash_sensor_daily;

CREATE VIEW rsingh_ogx_semantic.dash_sensor_daily
COMMENT 'Dashboard: per sensor and day, readings, breaches, stuck and late windows (gold.fact_sensor_daily)'
AS
SELECT s.business_date, s.sensor_tag, s.measurement, s.asset_id, a.tag, a.asset_class, a.facility_id,
       s.readings, s.mean_value, s.max_value, s.mean_slope_per_h, s.windows, s.breach_windows,
       s.stuck_windows, s.late_readings, s.bar_readings,
       CASE WHEN s.stuck_windows > 0 THEN 1 ELSE 0 END AS is_stuck,
       CASE WHEN s.bar_readings > 0 THEN 1 ELSE 0 END  AS has_unit_change
FROM rsingh_ogx_gold.fact_sensor_daily s
LEFT JOIN rsingh_ogx_gold.dim_asset a ON a.asset_id = s.asset_id AND a.is_current;
