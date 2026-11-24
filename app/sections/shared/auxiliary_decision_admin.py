"""Administrator workbook maintenance for published alerts."""

import logging

import streamlit as st

from app.utils.admin_mode import is_admin_mode
from src.shared_kernel.config import ConfigLoader
from src.shared_kernel.infrastructure.alert_decision_sheet import AlertDecisionInvalid, AlertDecisionSheet
from src.shared_kernel.infrastructure.alert_workbook import SnapshotChangedError

logger = logging.getLogger(__name__)


def render_decision_admin(layout: AlertDecisionSheet, domain: str, resource: str) -> None:
    if is_admin_mode():
        _render_editor(layout, domain, resource)


@st.fragment
def _render_editor(layout: AlertDecisionSheet, domain: str, resource: str) -> None:
    prefix = f'{resource}_{layout.sheet}'
    with st.expander(f'维护{layout.sheet}预警决定'):
        if st.session_state.pop(f'{prefix}_saved', False):
            st.success('已保存预警决定。')
        st.caption(f'在“{layout.decisions}”工作表修改 Flag。False 放行，True 屏蔽，Delete 从公开预警中移除。'
                   '保留“决策版本”工作表，上传时只更新列出的记录。其他人已修改决定时，请重新下载后编辑。')
        try:
            path = ConfigLoader.get_domain_resource_path(domain, resource)
            payload, version = layout.download(path)
        except Exception:
            logger.exception('Alert decision workbook unavailable: %s', layout.sheet)
            st.error('暂时无法读取预警决定。')
            return
        st.download_button('下载预警决定', data=payload, file_name=f'{layout.decisions}.xlsx',
                           key=f'{prefix}_download', on_click='ignore',
                           mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        upload = st.file_uploader('上传修改后的预警决定', type=['xlsx'], key=f'{prefix}_upload_{version}')
        if upload is None or not st.button('保存决定并刷新', type='primary', key=f'{prefix}_apply'):
            return
        try:
            layout.apply_upload(path, upload.getbuffer(), version)
        except (AlertDecisionInvalid, SnapshotChangedError) as exc:
            st.error(str(exc))
        except Exception:
            logger.exception('Alert decision workbook update failed: %s', layout.sheet)
            st.error('预警决定保存失败，请确认工作簿已关闭后重试。')
        else:
            st.session_state[f'{prefix}_saved'] = True
            st.rerun()
