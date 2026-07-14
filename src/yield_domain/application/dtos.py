import hashlib
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from src.shared_kernel.config_model import AppConfig


class YieldDataPolicy(BaseModel):
    """注入 Yield 数据提供者的应用级静态数据策略。"""

    work_order_types: tuple[str, ...] = Field(default_factory=tuple)
    target_defect_groups: tuple[str, ...] = Field(default_factory=tuple)

    model_config = ConfigDict(frozen=True)

    @classmethod
    def from_app_config(cls, config: "AppConfig") -> "YieldDataPolicy":
        """在应用边界将已校验配置转换为底层数据策略。"""
        return cls(
            work_order_types=tuple(config.data_source.work_order_types),
            target_defect_groups=tuple(config.data_source.target_defect_groups),
        )

    @property
    def signature(self) -> str:
        """返回可用于缓存和快照隔离的稳定策略签名。"""
        payload = self.model_dump_json().encode("utf-8")
        return hashlib.sha256(payload).hexdigest()[:12]


class YieldQueryConfig(BaseModel):
    """
    [Yield 域] 动态查询契约 (DTO)
    封装前端发起的良率数据查询请求参数。
    """
    start_date: str = Field(..., description="查询起始日期 (格式 YYYY-MM-DD)")
    end_date: str = Field(..., description="查询截止日期 (格式 YYYY-MM-DD)")
    product_code: str = Field(..., description="目标产品代码 (如 M626)")

    model_config = ConfigDict(extra="forbid")
