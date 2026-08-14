# Task：修正逻辑优化
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目：我们正在开发的是一款面向客户的企业级报表，需要进行数据修饰（不能释放真实数据）
- 场景：请帮助我优化当前报表Inline监控模块（Inline_domain）的数据修饰逻辑
- 目标：将aoi_tt/aoi_rs模块的修饰逻辑与spc/ctq模块对齐，包括以下两点：
    * 超规片自动修饰
    * 提供配置文件，可以指定sheet释放或删除

## Terms
如果遇到不理解的术语，可参考：`references\domain\GLOSSARY.md`

## Task1：方案设计
1. 请先分析当前spc和ctq的修饰逻辑，并输出一份解释文档到如下路径：`docs\dev_docs\generated\Inline_domain`
    - 二者在设计上应该是一致的；如果并不一致，请分析是否能够统一；如果不能统一，请解释理由

2. 请分析当前inline_model中APP层复用pipeline的逻辑：`src\inline_domain\application\shared\decorated_features.py`，并输出一份解释文档到如下路径：`docs\dev_docs\generated\Inline_domain`

3. 逻辑复用：Inline_dommain不同子模块的数据修饰逻辑极其相似，但每个子模块都重复编写，这并不合理。除了代码冗余之外，我们还需要确保算法的统一性，避免后续优化和排障复杂。因此，我认为我们应该将其作为一种通用算法纳入`shared`中。请分析我的这一想法，并输出一份文档至如下路径：`docs\dev_docs\generated\Inline_domain`
    - 当前aoi_tt/aoi_rs模块已经实现了超规截断的逻辑，并且已经将其放入了`src/inline_domain/core/shared/auto_decoration.py`