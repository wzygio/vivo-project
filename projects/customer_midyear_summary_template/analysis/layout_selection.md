# Layout Selection Rationale

Source deck: `sources/template-PPT.pptx`

The source file is enterprise-protected and cannot be parsed as a standard OOXML zip package. PowerPoint COM can open it, so this project uses a direct native-PowerPoint generation fallback instead of the `template_fill_pptx.py` OOXML path.

## Selected Base Layout

- Source slide/layout: `内容页_1`
- Reason: it is the only source slide, and its custom layout includes a top rule, title placeholder, right-side brand image, and left-edge accent blocks. It is best suited as a consistent content-page base for all three requested modules.

## Output Pages

1. 系统搭建
   - Base layout: `内容页_1`
   - Layout reason: a progress/timeline page fits system setup progress and can carry milestones plus current status cards.
   - Chart placeholder: milestone timeline with stage status markers.

2. 风险品处置
   - Base layout: `内容页_1`
   - Layout reason: a three-step risk handling funnel matches the requested indicators: triggered warnings, risk products, handled products.
   - Chart placeholder: conversion funnel with KPI cards and rate callouts.

3. 制程提升
   - Base layout: `内容页_1`
   - Layout reason: a before/after comparison plus improvement matrix fits CPK optimization, process optimization, and special-project progress.
   - Chart placeholder: before/after CPK bar chart with progress/matrix placeholders.
