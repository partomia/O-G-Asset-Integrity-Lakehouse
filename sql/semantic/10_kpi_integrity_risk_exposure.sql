-- Certified KPI: integrity risk exposure (ref.kpi_definition kpi_code = 'IRE').
-- Grain: asset x business date. p_event_30d is the equipment_risk probability; until the CAI
-- model scores land (gold.fact_model_score, Phase 10) it is the rule score / 100 from
-- gold.fact_asset_risk_daily, flagged by p_source. Criticality weights come from
-- ref.kpi_parameter. Assets with a stuck sensor in the last 3 days are abstained: they count
-- zero and are reported separately. Consumers aggregate this view:
--   exposure           = SUM(exposure)
--   abstained assets   = SUM(is_abstained)

DROP VIEW IF EXISTS rsingh_ogx_semantic.kpi_integrity_risk_exposure;

CREATE VIEW rsingh_ogx_semantic.kpi_integrity_risk_exposure
COMMENT 'Certified KPI IRE v1.0: asset x business date, p_event_30d x criticality weight, abstentions count zero'
AS
SELECT
    r.business_date,
    r.asset_id,
    r.tag,
    r.asset_class,
    r.facility_id,
    r.criticality,
    r.wall_loss_pct,
    r.breach_windows_3d,
    r.stuck_windows_3d,
    r.corrective_wo_30d,
    r.days_overdue,
    r.rule_score,
    r.risk_band,
    CAST(r.rule_score / 100 AS DOUBLE)                                   AS p_event_30d,
    'rule_score'                                                         AS p_source,
    COALESCE(w.value, 1.0)                                               AS criticality_weight,
    CASE WHEN COALESCE(r.stuck_windows_3d, 0) > 0 THEN 1 ELSE 0 END      AS is_abstained,
    CASE WHEN COALESCE(r.stuck_windows_3d, 0) > 0 THEN 'stuck sensor' END AS abstain_reason,
    CAST(CASE WHEN COALESCE(r.stuck_windows_3d, 0) > 0 THEN 0
              ELSE r.rule_score / 100 * COALESCE(w.value, 1.0) END AS DOUBLE) AS exposure
FROM rsingh_ogx_gold.fact_asset_risk_daily r
LEFT JOIN rsingh_ogx_ref.kpi_parameter w
       ON w.business_date = r.business_date AND w.parameter = concat('criticality_weight.', r.criticality);
