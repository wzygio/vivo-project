# Task：OLED_RS发生率统计
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 任务：请帮我根据预警数据，开发一套邮件自动推送服务

## References
1. 如果有不理解的词汇，请参考：`04-Resources\domain\GLOSSARY.md`
2. 数据探查与读取：如果数据量巨大，请分批捞取
    - 数据库链接例程：`tools\db_handler.py`
    - 数据库探查例程：`references\templates\data_loader.py`
	- 数据库链接凭证：`.env`
3. API相关信息：`references\domain\api\api-caller-guide-2026-11-03.md`
4. 邮件推送例程：

# Requirements
1. 数据获取：调用API，不填写任何参数（默认获取上周全部预警数据）
2. 架构：在`api`下设立一个sub_domain来承载该模块逻辑
3. 数据处理：根据不同模块不同厂别，推送给不同的责任人，具体逻辑如下(以下信息应当在`config\domain\api_domain.yaml`中可配置)

| sub_domain | factory | 推送邮箱 |
| --- | --- | --- |
| SPC | ARRAY/OLED/TP | jieqiang@visionox.com |
| CTQ | ARRAY/OLED/TP | jieqiang@visionox.com |
| AOI_TT/AOI_RS/Q-Time | ARRAY | sunyang01@visionox.com |
| AOI_TT/AOI_RS/Q-Time | OLED | liuzy08@visionox.com |
| AOI_TT/AOI_RS/Q-Time | TP | panxy01@visionox.com |
| Yield | ARRAY/OLED/TP | wangzy03@visionox.com | 


