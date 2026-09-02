# ADR-0021：指标监控领域及 Q-Time、IJP 同级子模块

- Status: Accepted
- Date: 2026-09-02
- Scope: `src/indicator_domain/`、`app/{sections,charts}/indicator_domain/`、
  `config/domain/indicator_domain.yaml`、`tests/**/indicator_domain/`
- Supersedes: ADR-0019、ADR-0020 中关于顶层 `qtime_domain` 及 IJP 从属于
  Q-Time 的目录决策；其数据源、安全和展示边界继续有效。

## Context

Q-Time 与 IJP 都属于制造指标监控能力，但两者具有独立的数据契约、领域规则和
展示流程。原结构以 Q-Time 命名顶层领域，并将 IJP 的文件不一致地散布在分层
根目录和 `application/ijp/` 中，容易造成 IJP 从属于 Q-Time 的错误认知，也不利于
继续加入新的指标模块。

## Decision

1. 顶层领域统一命名为 `indicator_domain`。
2. application、core、infrastructure 每层均以 `qtime/`、`ijp/` 作为同级子模块；
   文件在子模块内使用 `service.py`、`repository.py` 等局部语义名称。
3. `composition.py` 是领域唯一组合根，分别暴露 Q-Time 与 IJP 的服务构造函数。
4. Streamlit section、chart、测试和配置采用相同的 `indicator_domain/{qtime,ijp}`
   结构；Q-Time 人工修饰资源归入 `resources/indicator_domain/qtime/`。
5. 不保留 `src.qtime_domain` 兼容包。仓库内调用方必须迁移到新路径，防止新旧
   命名长期并存。

## Consequences

- Q-Time 与 IJP 的领域地位、依赖方向和测试归属更加明确。
- 新指标可以作为 `indicator_domain` 下的同级子模块加入，不必依附 Q-Time。
- 这是导入路径破坏性变更；仓库内页面、fixture 和测试必须一次性同步迁移。
- ADR-0019、ADR-0020 的数据库参数化、安全错误和展示层边界仍然有效。

## Verification

- 架构契约测试验证六个 application/core/infrastructure 子模块均可按新路径导入。
- Q-Time 与 IJP 的单元、Streamlit AppTest 和 SQL 集成测试必须全部通过。
- 引用审计不得在运行代码、配置或当前架构文档中残留 `qtime_domain` 导入路径。
