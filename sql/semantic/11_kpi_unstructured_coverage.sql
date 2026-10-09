-- Certified KPI: unstructured coverage (ref.kpi_definition kpi_code = 'UNC').
-- Grain: object x business date. Every object received counts in the denominator, quarantined
-- ones included; a resend of an object already held (DUPLICATE) is not a new object. Drone
-- flight sidecars (json) are metadata, not documents. A SEG-Y survey covers a field, not one
-- asset, so it counts as linked once its headers are extracted. Consumers aggregate:
--   coverage = SUM(is_covered) / SUM(is_received)

DROP VIEW IF EXISTS rsingh_ogx_semantic.kpi_unstructured_coverage;

CREATE VIEW rsingh_ogx_semantic.kpi_unstructured_coverage
COMMENT 'Certified KPI UNC v1.0: object x business date, extracted and linked to an asset over received'
AS
SELECT
    o._business_date                                                    AS business_date,
    o.doc_id,
    o.file_name,
    o.source,
    CASE WHEN o.source = 'drawings' THEN 'drawing' ELSE o.format END     AS format,
    o.ingest_status,
    o.reject_reason,
    d.asset_id,
    1                                                                    AS is_received,
    CASE WHEN o.ingest_status = 'VALID' THEN 1 ELSE 0 END                AS is_extracted,
    CASE WHEN o.ingest_status = 'VALID' AND (d.asset_id IS NOT NULL OR o.format = 'segy')
         THEN 1 ELSE 0 END                                               AS is_covered
FROM rsingh_ogx_bronze.doc_object o
LEFT JOIN (SELECT doc_id, business_date, MIN(asset_id) AS asset_id
           FROM rsingh_ogx_gold.fact_document GROUP BY doc_id, business_date) d
       ON d.doc_id = o.doc_id AND d.business_date = o._business_date
WHERE o.format <> 'json' AND o.ingest_status <> 'DUPLICATE';
