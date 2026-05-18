# Shared Kernel · 共享内核设计

> **领域代码**: `shared_kernel`  
> **对应目录**: [`src/shared_kernel/`](../../src/shared_kernel/)  
> **最后更新**: 2026-05-18

---

## 1. 概述

共享内核（Shared Kernel）是连接各业务域的通用基础设施层，提供配置管理、数据库连接、数据探针、文件处理等公共能力。采用 DDD 中 Shared Kernel 模式，确保各域之间的统一性和可维护性。

---

## 2. 模块构成

### 2.1 [`config.py`](../../src/shared_kernel/config.py) — 配置工厂

**角色**: `ConfigLoader` 静态配置工厂

**调用链**：
```
load_config(product_code)
  → 加载 .env
  → 加载 global.yaml
  → 加载 products/{product_code}.yaml
  → _deep_merge 深度合并
  → AppConfig.model_validate() Pydantic 校验
```

**特性**：
- 链式加载：全局配置 → 产品级配置
- 深度合并：产品级配置覆盖全局默认值
- Pydantic V2 校验：运行时类型安全
- 单例模式：通过 `@st.cache_resource` 隐式实现

### 2.2 [`config_model.py`](../../src/shared_kernel/config_model.py) — Pydantic 模型

| 模型 | 用途 |
|------|------|
| `AppConfig` | 顶层应用配置 |
| `FileResource` | 文件资源路径描述 |

配置链式访问示例：
```python
config.application.cache_ttl_hours        # 缓存TTL
config.data_source.product_code           # 产品代码
config.paths['static_warning_lines'].file_name  # 文件路径
config.processing['defect_capping']       # 处理参数
```

### 2.3 [`db_handler.py`](../../src/shared_kernel/infrastructure/db_handler.py) — 数据库连接

**角色**: `DatabaseManager` — SQLAlchemy 单例连接池

**设计要点**：
- `__new__` 单例模式：全局唯一数据库连接实例
- 失败重试机制：连接失败自动重试
- `.env` 延迟加载：运行时加载数据库凭证
- 断线重连：连接中断后自动恢复
- **红线**：禁止修改单例模式

### 2.4 [`data_inspector.py`](../../src/shared_kernel/utils/data_inspector.py) — 数据探针

**角色**: 调试与数据导出工具

**功能**：
- `export_probed_details()` — 全链路数据导出
- 配合 `spc_probe_targets.xlsx` 名单
- 支持单文件多 Sheet 导出
- 条件捕获：仅导出匹配条件的记录

### 2.5 [`excel_tools.py`](../../src/shared_kernel/utils/excel_tools.py) — Excel 工具

**角色**: xlsx→csv 转换（含加密文件 fallback）

**读取策略**：
1. **主路径（COM 解密）**：`_read_encrypted_xlsx_via_com()` 使用 `win32com.client.Dispatch('Excel.Application')`
2. **CSV 回退路径**：`resources/xlsx_to_csv/` 下的 CSV 备份
3. **自动降级**：遇到 `BadZipFile`（加密）时自动 fallback 到 csv

---

## 3. 日志架构

实现于 [`app/utils/logger_setup.py`](../../app/utils/logger_setup.py:10)。

### 3.1 领域分流（纵轴）

使用 `DomainFilter` 根据代码文件路径自动将日志分流：

| 日志文件 | 领域 |
|----------|------|
| `app_yield.log` | `yield_domain` |
| `app_spc.log` | `spc_domain` |
| `app_shared.log` | `shared_kernel` |

### 3.2 级别隔离（横轴）

| 日志文件 | 级别 | 保留期 |
|----------|------|--------|
| `app_info.log` | INFO+ | 30天 |
| `app_error.log` | WARNING+ | 90天 |
| `app_trace.log` | DEBUG | 7天 |

所有 Handler 使用 `TimedRotatingFileHandler` 按天轮转。

---

## 4. 缓存体系

### 4.1 L1: Parquet 快照

- 路径：`data/{product_code}/yield_snapshot_{product_code}.parquet`
- TTL：8 小时
- 增量更新：最近 2 天缓冲窗口
- 降级策略：三防线容灾

### 4.2 L2: `@st.cache_data`

- 对 Service 层方法进行内存缓存
- 缓存键：`snapshot_signature` = MD5(文件 mtime+size)
- 自动失效：快照变更时自动驱逐

### 4.3 代码热重载

- `deep_reload_modules()` — 强制卸载 `src/`、`app/` 模块
- `get_project_revision()` — MD5 指纹用于 composite_key 缓存失效

---

## 5. 安全与凭证

- 数据库凭证存储于 `.env`（被 `.gitignore` 排除）
- 通过 `load_dotenv(override=True)` 加载为进程环境变量
- 密码通过 `quote_plus()` URL 编码防止 URI 破坏

---

> **相关文件**: [`ARCHITECTURE.md`](../../ARCHITECTURE.md) · [`yield_domain.md`](./yield_domain.md) · [`spc_domain.md`](./spc_domain.md)
