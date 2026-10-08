# Drone frame library: attribution

Every real frame here is a resized copy of an image from the TEST split of
**Corrosion Bi3Q3** (Roboflow 100 benchmark, https://universe.roboflow.com/roboflow-100/corrosion-bi3q3),
mirrored on Hugging Face as `LibreYOLO/corrosion-bi3q3` at revision
`d20b5b1e6b21ff7de555bbf848dcf7d30ccf8ee7`, licensed **CC BY 4.0**
(https://creativecommons.org/licenses/by/4.0/).

Changes: resized to 320 px wide and re-encoded as JPEG; frames with a non-empty `degraded`
column in `frames.csv` were further altered (blur, low light, glare, occlusion or noise) by
`features/degrade.py`. Severity labels are derived from the dataset's boxes by
`features/labels.py`. Built by `scripts/build_frame_library.py`.
