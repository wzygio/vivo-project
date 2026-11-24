"""Download and update lifetime alert decisions in admin mode."""

import logging

import streamlit as st
from app.utils.admin_mode import is_admin_mode

from src.iqc_domain.composition import build_lifetime_alert_store
from src.iqc_domain.core.lifetime.alerts import LifetimeLedgerInvalid
from src.shared_kernel.infrastructure.alert_workbook import SnapshotChangedError
from src.shared_kernel.config import ConfigLoader

logger = logging.getLogger(__name__)


def render_lifetime_ledger_admin() -> None:
    if not is_admin_mode():
        return
    if 'lifetime_ledger_saved' in st.session_state:
        st.success(f"已保存寿命预警台账，共 {st.session_state.pop('lifetime_ledger_saved')} 条记录。")
    with st.expander('维护寿命预警台账'):
        st.caption('在“寿命预警”工作表修改 Flag。False 放行本期超规项，True 屏蔽，Delete 从公开预警中移除。'
                   '每日巡检更新规格点数值和“本期状态”，并保留同一项的 Flag。')
        st.caption('请保留“决策版本”工作表。上传仅更新已发布记录的决定。其他人已修改决定时，请重新下载后编辑。')
        try:
            store = build_lifetime_alert_store()
            payload, version = store.download()
        except Exception:
            logger.exception('Lifetime ledger download unavailable')
            st.error('暂时无法读取寿命预警台账。')
            return
        st.download_button('下载预警台账', data=payload, file_name=store.path.name,
                           mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                           on_click='ignore')
        uploaded = st.file_uploader('上传修改后的预警台账', type=['xlsx'], key=f'lifetime_ledger_upload_{version}')
        if uploaded is None or not st.button('保存决定并刷新', key='lifetime_ledger_apply', type='primary'):
            return
        try:
            count = store.replace_uploaded(uploaded.getbuffer(), expected_digest=version,
                                           products=tuple(ConfigLoader.get_enabled_products(strict=True)))
        except (LifetimeLedgerInvalid, SnapshotChangedError) as exc:
            st.error(str(exc))
        except PermissionError:
            st.error('台账正在使用或无法写入，请关闭打开的工作簿后重试。')
        except Exception:
            logger.exception('Lifetime ledger replacement failed')
            st.error('台账保存失败，请检查上传文件后重试。')
        else:
            st.session_state.lifetime_ledger_saved = count
            st.rerun()
