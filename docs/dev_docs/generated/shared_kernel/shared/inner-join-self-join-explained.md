# 为什么两条 `INNER JOIN` SQL 得到不同的长度分布

## 先记住一个结论

`INNER JOIN` 做的事情不是“把右表附加到左表”，而是：

> 从两边各取一行，组成一对；只保留满足 `ON` 条件的行对。

在这两个查询中，连接产生的父子标签配对实际上相同。结果不同的真正原因是：

- 第一条 SQL 对**父标签**的 `LABELSN` 计算长度；
- 第二条 SQL 对**子标签**的 `LABELSN` 计算长度。

`INNER JOIN` 的书写先后不是这里的关键。

---

## 1. 先理解：同一张表可以在一次查询中扮演两个角色

两个查询都把 `DWR_WMS_TBLRECLABEL` 写了两遍：

```sql
FROM DWR_WMS_TBLRECLABEL dwt
INNER JOIN DWR_WMS_TBLRECLABEL dwt1
```

这叫作 **self join（自连接）**。

可以把它想象成：数据库临时把同一张表复印了两份，然后分别贴上名字：

```text
同一张物理表
├── 临时角色 dwt
└── 临时角色 dwt1
```

`dwt.LABELSN` 和 `dwt1.LABELSN` 虽然来自同一个物理字段，但可以来自**不同的数据行**。

别名本身没有固定含义：`dwt` 不天然代表父标签，`dwt1` 也不天然代表子标签。它们在查询中扮演什么角色，是由 `ON` 和 `WHERE` 中如何使用它们决定的。

---

## 2. 用三行模拟数据理解父子关系

假设表中只有以下三行：

| LABELSN | PACKBOXNO | LABELLEVEL | ACCOUTSTATUS | INVENTORYCODE |
|---|---|---:|---|---|
| `PARENT123456` | `NULL` | 3 | `lm_deliveried` | `2F91` |
| `C001` | `PARENT123456` | 2 | 其他状态 | `2F91` |
| `C002` | `PARENT123456` | 2 | 其他状态 | `2F91` |

从数据含义看：

```text
父标签 PARENT123456
├── 子标签 C001，其 PACKBOXNO = PARENT123456
└── 子标签 C002，其 PACKBOXNO = PARENT123456
```

也就是说，子标签通过自己的 `PACKBOXNO` 指向父标签的 `LABELSN`：

```text
子标签.PACKBOXNO = 父标签.LABELSN
```

注意示例中的长度：

```text
CHAR_LENGTH('PARENT123456') = 12
CHAR_LENGTH('C001')         = 4
CHAR_LENGTH('C002')         = 4
```

---

## 3. `INNER JOIN` 到底如何产生结果

先看第一条 SQL 的连接条件：

```sql
dwt.LABELSN = dwt1.PACKBOXNO
```

数据库会尝试把 `dwt` 中的每一行与 `dwt1` 中的每一行配对，然后判断这个等式是否成立。

部分尝试过程如下：

| dwt.LABELSN | dwt1.LABELSN | dwt1.PACKBOXNO | `dwt.LABELSN = dwt1.PACKBOXNO` |
|---|---|---|---|
| `PARENT123456` | `PARENT123456` | `NULL` | 不成立 |
| `PARENT123456` | `C001` | `PARENT123456` | 成立 |
| `PARENT123456` | `C002` | `PARENT123456` | 成立 |
| `C001` | `PARENT123456` | `NULL` | 不成立 |
| `C001` | `C002` | `PARENT123456` | 不成立 |

`INNER JOIN` 只留下条件成立的行对，因此连接后的中间结果是：

| dwt.LABELSN | dwt.LABELLEVEL | dwt1.LABELSN | dwt1.PACKBOXNO |
|---|---:|---|---|
| `PARENT123456` | 3 | `C001` | `PARENT123456` |
| `PARENT123456` | 3 | `C002` | `PARENT123456` |

这个中间结果非常重要：每一行同时包含一个父标签和一个子标签。

在这个结果中：

```text
dwt  是父标签所在的行
dwt1 是子标签所在的行
```

这不是因为 `dwt` 写在前面，而是因为连接条件写成了：

```sql
dwt.LABELSN = dwt1.PACKBOXNO
```

`dwt1.PACKBOXNO` 指向 `dwt.LABELSN`，所以 `dwt1` 是子，`dwt` 是父。

---

## 4. 第一条 SQL 为什么统计了父标签长度

第一条 SQL 可以按执行逻辑拆成四步。

### 第一步：建立父子配对

```sql
FROM DWR_WMS_TBLRECLABEL dwt
INNER JOIN DWR_WMS_TBLRECLABEL dwt1
    ON dwt.LABELSN = dwt1.PACKBOXNO
```

得到：

| dwt，即父标签 | dwt1，即子标签 |
|---|---|
| `PARENT123456` | `C001` |
| `PARENT123456` | `C002` |

### 第二步：用 `WHERE` 筛选父标签

```sql
WHERE dwt.ACCOUTSTATUS = 'lm_deliveried'
  AND dwt.LABELLEVEL = 3
  AND dwt.INVENTORYCODE IN (...)
```

所有条件都写在 `dwt` 上，因此检查的是父标签行。

示例中的 `PARENT123456` 满足条件，所以两对关系都会留下。

### 第三步：计算 `dwt.LABELSN` 的长度

```sql
CHAR_LENGTH(dwt.LABELSN)
```

`dwt` 是父标签，所以计算过程为：

| 父标签 | 子标签 | 实际计算的值 | 长度 |
|---|---|---|---:|
| `PARENT123456` | `C001` | `PARENT123456` | 12 |
| `PARENT123456` | `C002` | `PARENT123456` | 12 |

### 第四步：分组和计数

```sql
GROUP BY CHAR_LENGTH(dwt.LABELSN)
```

最终得到：

| LABELSN_LENGTH | COUNT(*) |
|---:|---:|
| 12 | 2 |

这里的 `2` 不是说有两个不同的父标签，而是说连接结果有两行，也就是一个父标签与两个子标签形成了两对关系。

---

## 5. 第二条 SQL 为什么统计了子标签长度

第二条 SQL 的连接条件是：

```sql
T.PACKBOXNO = T1.LABELSN
```

它表达的仍然是：

```text
子标签.PACKBOXNO = 父标签.LABELSN
```

所以此时：

```text
T  是子标签
T1 是父标签
```

连接后的中间结果是：

| T，即子标签 | T.PACKBOXNO | T1，即父标签 |
|---|---|---|
| `C001` | `PARENT123456` | `PARENT123456` |
| `C002` | `PARENT123456` | `PARENT123456` |

然后 `WHERE` 条件写在 `T1` 上：

```sql
WHERE T1.ACCOUTSTATUS = 'lm_deliveried'
  AND T1.LABELLEVEL = 3
  AND T1.INVENTORYCODE IN (...)
```

因此仍然是在筛选父标签。

但是 `SELECT` 计算的是：

```sql
CHAR_LENGTH(T.LABELSN)
```

`T` 是子标签，所以实际计算过程为：

| 父标签 | 子标签 | 实际计算的值 | 长度 |
|---|---|---|---:|
| `PARENT123456` | `C001` | `C001` | 4 |
| `PARENT123456` | `C002` | `C002` | 4 |

最终得到：

| LABELSN_LENGTH | COUNT(*) |
|---:|---:|
| 4 | 2 |

这就是第二条 SQL 得到正确结果的原因：它先找到符合条件的三级父标签，再统计这些父标签下面的子标签 `LABELSN` 长度。

---

## 6. 两条 SQL 其实只差了“对哪一侧取长度”

把第一条 SQL 的别名替换成第二条 SQL 的别名：

```text
第一条 dwt  = 第二条 T1 = 父标签
第一条 dwt1 = 第二条 T  = 子标签
```

第一条 SQL 就相当于：

```sql
SELECT CHAR_LENGTH(T1.LABELSN)  -- 父标签长度
FROM DWR_WMS_TBLRECLABEL T1
INNER JOIN DWR_WMS_TBLRECLABEL T
    ON T1.LABELSN = T.PACKBOXNO
WHERE T1.ACCOUTSTATUS = 'lm_deliveried'
  AND T1.LABELLEVEL = 3;
```

第二条 SQL 相当于：

```sql
SELECT CHAR_LENGTH(T.LABELSN)   -- 子标签长度
FROM DWR_WMS_TBLRECLABEL T1
INNER JOIN DWR_WMS_TBLRECLABEL T
    ON T1.LABELSN = T.PACKBOXNO
WHERE T1.ACCOUTSTATUS = 'lm_deliveried'
  AND T1.LABELLEVEL = 3;
```

两者的连接和筛选相同，唯一关键差异是：

```diff
- CHAR_LENGTH(T1.LABELSN)  -- 父标签
+ CHAR_LENGTH(T.LABELSN)   -- 子标签
```

---

## 7. 你的理解哪里正确，哪里需要修正

你的观察是：

> 第一条 SQL 中，写在前面的 `dwt.LABELSN` 是父标签；第二条 SQL 中，写在前面的 `T.LABELSN` 是子标签。因此，可能是 `INNER JOIN` 的先后顺序改变了字段含义。

这个观察中的**现象是正确的**：

```text
第一条：第一个别名 dwt = 父标签
第二条：第一个别名 T   = 子标签
```

但“字段位置导致字段含义变化”这个因果关系不正确。这里需要把三个概念分开：

```text
1. 表在 FROM/JOIN 中的书写位置
2. SELECT 结果中列的显示位置
3. 别名在 ON、WHERE 和 SELECT 中承担的语义角色
```

### 7.1 书写顺序可能影响 `SELECT *` 的列显示顺序

例如：

```sql
SELECT *
FROM DWR_WMS_TBLRECLABEL parent_label
INNER JOIN DWR_WMS_TBLRECLABEL child_label
    ON child_label.PACKBOXNO = parent_label.LABELSN;
```

很多数据库会先展示 `parent_label` 的全部列，再展示 `child_label` 的全部列。如果把表的书写顺序交换：

```sql
SELECT *
FROM DWR_WMS_TBLRECLABEL child_label
INNER JOIN DWR_WMS_TBLRECLABEL parent_label
    ON child_label.PACKBOXNO = parent_label.LABELSN;
```

`SELECT *` 的两组列通常也会交换显示位置。因此，你所说的“字段先后顺序发生变化”在这个层面是成立的。

不过，原来的两个查询都没有使用 `SELECT *`，而是明确写了：

```sql
dwt.LABELSN
```

或者：

```sql
T.LABELSN
```

这种写法不是按结果中的第几个字段取值。数据库先根据别名找到对应的表实例，再读取那个实例的 `LABELSN`：

```text
dwt.LABELSN = 从别名 dwt 所代表的行中取 LABELSN
T.LABELSN   = 从别名 T 所代表的行中取 LABELSN
```

无论这个别名写在 `FROM` 后还是 `INNER JOIN` 后，引用都不会因为显示位置变化而指向另一行。

### 7.2 真正发生的变化是“第一个别名被分配了不同角色”

第一条查询的关键部分是：

```sql
FROM DWR_WMS_TBLRECLABEL dwt
INNER JOIN DWR_WMS_TBLRECLABEL dwt1
    ON dwt.LABELSN = dwt1.PACKBOXNO
WHERE dwt.LABELLEVEL = 3
```

这里：

```text
dwt1.PACKBOXNO 指向 dwt.LABELSN
WHERE 又要求 dwt 是三级标签
所以 dwt 是父标签，dwt1 是子标签
```

第二条查询的关键部分是：

```sql
FROM DWR_WMS_TBLRECLABEL T
INNER JOIN DWR_WMS_TBLRECLABEL T1
    ON T.PACKBOXNO = T1.LABELSN
WHERE T1.LABELLEVEL = 3
```

这里：

```text
T.PACKBOXNO 指向 T1.LABELSN
WHERE 又要求 T1 是三级标签
所以 T 是子标签，T1 是父标签
```

两条 SQL 确实都选择了“写在前面的别名”的 `LABELSN`：

```text
第一条选择第一个别名 dwt.LABELSN → 恰好是父标签
第二条选择第一个别名 T.LABELSN   → 恰好是子标签
```

因此，**书写位置与结果差异存在相关性，但不是直接原因**。直接原因是：

1. 两条 SQL 通过 `ON` 和 `WHERE` 给第一个别名分配了相反的父子角色；
2. 两条 SQL 又都在 `SELECT` 中读取第一个别名的 `LABELSN`。

### 7.3 一个对照实验：只交换书写顺序，结果不会变化

以下查询把子标签写在前面：

```sql
SELECT child_label.LABELSN
FROM DWR_WMS_TBLRECLABEL child_label
INNER JOIN DWR_WMS_TBLRECLABEL parent_label
    ON child_label.PACKBOXNO = parent_label.LABELSN
WHERE parent_label.ACCOUTSTATUS = 'lm_deliveried'
  AND parent_label.LABELLEVEL = 3;
```

下面把父标签写在前面，但保留相同别名及相同角色：

```sql
SELECT child_label.LABELSN
FROM DWR_WMS_TBLRECLABEL parent_label
INNER JOIN DWR_WMS_TBLRECLABEL child_label
    ON child_label.PACKBOXNO = parent_label.LABELSN
WHERE parent_label.ACCOUTSTATUS = 'lm_deliveried'
  AND parent_label.LABELLEVEL = 3;
```

两个查询都会输出相同的子标签，因为：

```text
child_label 始终是 PACKBOXNO 指向父标签的那一行
parent_label 始终是满足三级、已交付条件的那一行
SELECT 始终明确读取 child_label.LABELSN
```

这个实验说明：仅仅交换 `FROM` 与 `INNER JOIN` 两侧的书写位置，不足以让父标签变成子标签。

### 7.4 用一句话精确表述这次差异

不够精确的说法是：

```text
INNER JOIN 的先后顺序改变了 LABELSN 的含义。
```

更精确的说法是：

```text
两条 SQL 给第一个表别名分配了不同的父子角色，
并且都选择了第一个别名的 LABELSN，因而统计了不同对象。
```

对于等值 `INNER JOIN`，交换两侧顺序但保持别名、连接关系、筛选对象和选择对象不变，符合条件的逻辑行对不会改变。数据库的实际执行计划也可能自行调整连接顺序。

另外，`LEFT JOIN` 与 `RIGHT JOIN` 有“必须保留哪一侧”的含义，书写顺序会影响结果。本文讨论的是 `INNER JOIN`。

---

## 8. 建议先取消聚合，直接观察 JOIN 结果

当一个 JOIN 查询难以理解时，不要立刻使用 `COUNT(*)` 和 `GROUP BY`。先同时输出两侧字段：

```sql
SELECT
    T1.LABELSN AS PARENT_LABELSN,
    T1.LABELLEVEL AS PARENT_LEVEL,
    T1.ACCOUTSTATUS AS PARENT_STATUS,
    T.LABELSN AS CHILD_LABELSN,
    T.PACKBOXNO AS CHILD_PACKBOXNO,
    CHAR_LENGTH(T1.LABELSN) AS PARENT_LABELSN_LENGTH,
    CHAR_LENGTH(T.LABELSN) AS CHILD_LABELSN_LENGTH
FROM DWR_WMS_TBLRECLABEL T
INNER JOIN DWR_WMS_TBLRECLABEL T1
    ON T.PACKBOXNO = T1.LABELSN
WHERE T1.ACCOUTSTATUS = 'lm_deliveried'
  AND T1.LABELLEVEL = 3
  AND T1.INVENTORYCODE IN ('2F91', '3F91', '9F91', '9F92', 'ZF04')
LIMIT 50;
```

查看结果时，逐列确认：

```text
T1.LABELSN     是不是三级父标签？
T.PACKBOXNO    是否等于 T1.LABELSN？
T.LABELSN      是不是你真正想提取的子标签？
```

确认这些关系后，再加上 `GROUP BY`。

---

## 9. 最终推荐查询

如果需求是：

> 找到状态为 `lm_deliveried`、层级为 3、库存编码在指定范围内的父标签，然后统计其下级标签 `LABELSN` 的长度分布。

那么第二条 SQL 的方向正确。可以使用更明确的别名，减少理解成本：

```sql
SELECT
    CHAR_LENGTH(child_label.LABELSN) AS LABELSN_LENGTH,
    COUNT(*) AS RELATION_COUNT
FROM DWR_WMS_TBLRECLABEL child_label
INNER JOIN DWR_WMS_TBLRECLABEL parent_label
    ON child_label.PACKBOXNO = parent_label.LABELSN
WHERE parent_label.ACCOUTSTATUS = 'lm_deliveried'
  AND parent_label.LABELLEVEL = 3
  AND parent_label.INVENTORYCODE IN ('2F91', '3F91', '9F91', '9F92', 'ZF04')
GROUP BY CHAR_LENGTH(child_label.LABELSN)
ORDER BY LABELSN_LENGTH;
```

使用 `parent_label` 和 `child_label`，比 `T`、`T1` 或 `dwt`、`dwt1` 更容易看出每个字段属于哪种角色。

### `COUNT(*)` 统计的是什么

这里的 `COUNT(*)` 统计连接后的父子关系行数。如果同一个子标签可能因为数据重复而出现多次，而你只想统计唯一子标签数量，可以改成：

```sql
COUNT(DISTINCT child_label.LABELSN) AS UNIQUE_LABEL_COUNT
```

完整写法为：

```sql
SELECT
    CHAR_LENGTH(child_label.LABELSN) AS LABELSN_LENGTH,
    COUNT(DISTINCT child_label.LABELSN) AS UNIQUE_LABEL_COUNT
FROM DWR_WMS_TBLRECLABEL child_label
INNER JOIN DWR_WMS_TBLRECLABEL parent_label
    ON child_label.PACKBOXNO = parent_label.LABELSN
WHERE parent_label.ACCOUTSTATUS = 'lm_deliveried'
  AND parent_label.LABELLEVEL = 3
  AND parent_label.INVENTORYCODE IN ('2F91', '3F91', '9F91', '9F92', 'ZF04')
GROUP BY CHAR_LENGTH(child_label.LABELSN)
ORDER BY LABELSN_LENGTH;
```

---

## 10. 最后用一句伪代码记住整个查询

第二条 SQL 可以翻译成：

```text
对于表中的每一个子标签 T：
    找到 LABELSN 等于 T.PACKBOXNO 的父标签 T1；
    只保留满足状态、层级和库存编码条件的父标签；
    读取子标签 T.LABELSN；
    按子标签字符长度分组计数。
```

最重要的三个判断点是：

```text
ON     决定哪些行组成父子配对
WHERE  决定保留哪些配对
SELECT 决定最终从配对的哪一侧读取字段
```

你的第二条 SQL 正确，不是因为 `INNER JOIN` 的左右顺序正确，而是因为：

```text
WHERE 使用 T1 筛选父标签
SELECT 使用 T 读取子标签
```
