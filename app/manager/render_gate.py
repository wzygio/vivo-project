"""[共享能力] 两阶段渲染闸门 (Render Gate)

Streamlit 的执行模型是"脚本自上而下、元素即建即推"：每个 st.* 调用一旦执行，
对应的 delta 立即推送浏览器渲染。若页面把"重计算（如构建 Plotly 图）"与
"st.* 渲染调用"交错执行，图表就会一张一张跳出来，造成页面持续抖动、操作卡顿。

RenderGate 将两者解耦：
    阶段 1 (collect): 在统一 spinner 下集中执行全部纯计算任务；
    阶段 2 (flush):   计算结果就绪后，页面再集中执行 st.* 渲染调用，
                      此时各元素之间没有计算间隔，图表近乎一次性铺满。

使用约定：
    - stage() 注册的任务必须是纯计算（不得包含任何 st.* 渲染调用）；
    - collect() 返回结果列表后，由调用方自行遍历并执行渲染。
"""

from collections.abc import Callable
from typing import Any

import streamlit as st

DEFAULT_SPINNER_TEXT = "正在准备图表，请稍候…"


class RenderGate:
    """收集纯计算任务，在统一 spinner 下批量执行，供页面随后一次性渲染。"""

    def __init__(self, spinner_text: str = DEFAULT_SPINNER_TEXT) -> None:
        self._spinner_text = spinner_text
        self._jobs: list[Callable[[], Any]] = []

    def stage(self, job: Callable[[], Any]) -> None:
        """注册一个纯计算任务（禁止包含 st.* 渲染调用）。"""
        self._jobs.append(job)

    @property
    def pending_count(self) -> int:
        return len(self._jobs)

    def collect(self) -> list[Any]:
        """在统一 spinner 下执行全部任务并返回结果列表，队列随即清空。"""
        jobs = self._jobs
        self._jobs = []
        if not jobs:
            return []
        with st.spinner(self._spinner_text):
            return [job() for job in jobs]
