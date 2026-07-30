# Vocabulary
以下是OLED显示屏制造公司的专有词汇

## LOT/SHEET/GLASS/PANEL及相关词汇
- Lot：生产单元，一般一个lot包含30个sheet。ID为9位。
- Sheet：显示屏在前段工艺中是一整个大板，我们称之为Sheet。ID为11位，前9位为Lot ID。
- Glass：Sheet在蒸镀工艺段被一切为二。ID为12位，前11为Sheet ID。
- Panel：在屏体段（最后一道工艺）大板才会被切割成手机尺寸的大小，切割后的屏体我们称之为Panel。ID为15位，前11为SheetID，后四位为坐标。
- 裁切数：每一个Sheet可以切割成的Panel数。
    * 一般Sheet的尺寸是不变的，一般裁切数越小说明Panel尺寸越大，价值越高。
    * 计算方式：由一个X向裁切数和Y向裁切数相乘得来
    * 命名方式：数据库中，X向裁切数和Y向裁切数可能的命名方式有：“PRODUCTCOUNTTOXAXIS，PRODUCTCOUNTTOYAXIS”。并往往伴随一个glass裁切数，它是sheet裁切数的一般，可能的命名有“SUBPRODUCTUNITQUANTITY1”
- Mapping：将每一个Panel根据位置坐标拼接成一整个大板来寻找前端工艺的问题，是一种异常调查的常见手段。而拼接成的大板就叫做Mapping。注意，这里的拼接是在数据维度上，因此可以根据需求拼接。
- 膜位：Mapping图上Panel所在的位置就叫做膜位。比如我们可以将所有的panel拼接成Mapping图，来观察哪个膜位的不良较为显著。

## Defect
- 异常：产品的缺陷，又称“缺陷/不良”
- Defect Group：异常分组，一个分组包含多个Defect Code
- Defect Code：异常代码
- CT：屏体厂的核心检测站点

## 工艺
- 四大工艺：OLED显示屏制造有四大工艺，分别为“ARRAY”, "OLED", "TP", "CELL" 。
    * ARRAY：阵列，制作TFT背板
    * OLED：制作有机发光层，又称“蒸镀/EVA(Evaporation)”
    * TP：Touch Panel，制作触控屏
    * CELL：屏体封装，CT检测就在这里，CT不良打出的地方。
- 厂别：每道工艺对应一个工厂。因此有时候“工艺”又称“工厂”。

## SPC
- 监控指标：“站点-参数”的组合。每个站点可能会测多个参数，但每个参数可能会在多个站点测量，所以只有组合才能锁定唯一一个监控指标
- SPC：统计过程控制。对于我们公司来说，具体形式为：
    * 针对每个监控指标，在一个sheet上的多个点位进行测量，取均值作为该sheet的测量值。
    * 每个指标都有上下限，通过“自动预警报表”进行监控。
