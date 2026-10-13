"""Material/cycle membership from product-request master data, including unshipped work."""
import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.sql.elements import TextClause


MATERIAL_PRODUCT_SQL = """
WITH material_batches AS (
    SELECT DISTINCT l.PLANORGANICSPEC AS material_no, l.CYCLETIME AS batch_no
    FROM mdw.dwr_mes_ct_cruciblelothistory l
    WHERE l.PLANORGANICSPEC IN :materials
      AND l.EVENTNAME = 'KitBatchCrucible'
      AND l.MACHINENAME != '3CEE03' AND l.CYCLETIME IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM mdw.dwr_mes_consumablehistory h
          WHERE h.CRUCIBLELOTNAME = l.CRUCIBLELOTNAME
            AND h.EVENTNAME = 'KitBatchCrucible'
      )
), batch_requests AS (
    SELECT mb.material_no, mb.batch_no, r.PRODUCTSPECNAME, r.SUBPRODUCTIONTYPE
    FROM material_batches mb JOIN mdw.dwr_mes_productrequest r
      ON mb.batch_no = to_char(to_date(
          substring(r.DESCRIPTION, '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}'),
          'YYYY/MM/DD'), 'YYYYMMDD')
)
SELECT DISTINCT b.material_no, p.PRODUCTCODE AS prod_code, b.batch_no
FROM batch_requests b
JOIN mdw.dwr_mes_productspec p ON p.PRODUCTSPECNAME = b.PRODUCTSPECNAME
WHERE p.PRODUCTCODE IN :products AND b.SUBPRODUCTIONTYPE IN :work_orders
"""


def material_product_statement() -> TextClause:
    """Bind all externally supplied collections; keep date parsing in PostgreSQL."""
    return text(MATERIAL_PRODUCT_SQL).bindparams(
        bindparam("materials", expanding=True),
        bindparam("products", expanding=True),
        bindparam("work_orders", expanding=True),
    )


def project_material_scope(
    source: pd.DataFrame, membership: pd.DataFrame, products: tuple[str, ...],
) -> pd.DataFrame:
    """Attach selected product/cycle sets without expanding inspection facts."""
    fields = ["material_no", "prod_code", "batch_no"]
    if not set(fields).issubset(membership.columns):
        raise ValueError("IQC_PRODUCT_SCOPE_SCHEMA_INVALID")
    selected = membership.loc[membership.prod_code.isin(products), fields].drop_duplicates()
    if selected.isna().any(axis=None):
        raise ValueError("IQC_PRODUCT_SCOPE_SCHEMA_INVALID")
    product_labels: dict[str, str] = {}
    batch_labels: dict[str, str] = {}
    for material, group in selected.groupby("material_no", sort=False):
        codes = set(group.prod_code)
        product_labels[material] = "、".join(code for code in products if code in codes)
        batch_labels[material] = "、".join(sorted(set(group.batch_no.astype(str))))
    frame = source.loc[source.mitemname.isin(product_labels)].copy()
    frame["prod_code"] = frame.mitemname.map(product_labels)
    frame["batch_no"] = frame.mitemname.map(batch_labels)
    return frame
