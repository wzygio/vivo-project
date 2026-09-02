# SQL语句：IJP溢流报表
以下是IJP溢流报表的SQL语句。在帆软报表中，每个数据集对应的sql语句分别如下：

## 重点语句
1. `CYCLE`：
```
SELECT CYCLE_ID
  FROM EDA.DWD_GLASS_OLED_CYCLE_V3 D 
 WHERE 1=1
   AND EVENT_TIME::TIMESTAMP >= '${dtStartDate}'::TIMESTAMP
   AND EVENT_TIME::TIMESTAMP <= '${dtEndDate}'::TIMESTAMP
   AND EVENT_TIME <> 'NaT'
   ${IF(LEN(cmcbLot) = 0,"", "AND PICI IN ('" + cmcbLot + "')" )}
   ${IF(LEN(cmcbProdCode) = 0,"", "AND PROD_CODE IN ('" + cmcbProdCode + "')" )}
 GROUP BY CYCLE_ID
 ORDER BY CYCLE_ID

```

2. `FACTORY`：
```
SELECT
	FACTORY
FROM
	DWR_MES_MESFACTORY_V
where factory ='OLED'

```

3. `PICI`：
```
SELECT PICI
  FROM EDA.DWD_GLASS_OLED_CYCLE_V3 D 
 WHERE 1=1
   AND EVENT_TIME::TIMESTAMP >= '${dtStartDate}'::TIMESTAMP
   AND EVENT_TIME::TIMESTAMP <= '${dtEndDate}'::TIMESTAMP
   AND EVENT_TIME <> 'NaT'
   ${IF(LEN(cmcbProdCode) = 0,"", "AND PROD_CODE IN ('" + cmcbProdCode + "')" )}
 GROUP BY PICI
 ORDER BY PICI

```

4. `PRODCODE`：
```
select distinct 
product_code 
from DWR_MES_PRODUCTSPEC_V
WHERE 1=1
and factory like '%OLED%'
order by product_code

```

5. `PRODUCT`：
```
SELECT
  PROD_ID
FROM
  DWR_MES_PRODUCTSPEC_V
WHERE 1=1
${IF(LEN(cmcbFactory)=0,"","AND FACTORY IN ('" + cmcbFactory + "')")}
${IF(LEN(cmcbProdCode)=0,"","AND product_code IN ('"+cmcbProdCode+"')")}
ORDER BY PROD_ID
```

6. `SEARCH`：
```
SELECT
  PROD_ID
FROM
  DWR_MES_PRODUCTSPEC_V
WHERE 1=1
${IF(LEN(cmcbFactory)=0,"","AND FACTORY IN ('" + cmcbFactory + "')")}
${IF(LEN(cmcbProdCode)=0,"","AND product_code IN ('"+cmcbProdCode+"')")}
ORDER BY PROD_ID

SELECT A.GLASS_START_TIME 
      ,A.GLASS_ID
      ,A.SUB_EQUIP_ID
      ,A.PANEL_ID 
	  ,A.PANEL_LOCATION
	  ,A.RS_CODE 
      ,A.CODE_NUM 
      ,A.TOTAL_CODE
      ,A.CODE_RATIO
      ,A.PRODUCTCODE 
      ,A.ID
--      ,A.PICI
  FROM (
SELECT D.GLASS_START_TIME 
      ,D.GLASS_ID
      ,H.SUB_EQUIP_ID
      ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14) AS PANEL_ID 
	  ,  CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LT' THEN 'LEFTTOP '
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='T0' THEN 'TOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RT' THEN 'RIGHTTOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='R0' THEN 'RIGHT' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RB' THEN 'RIGHTBOTTOM' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9') THEN 'BOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LB' THEN 'LEFTBOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='L0' THEN 'LEFT'
		     ELSE NULL END AS PANEL_LOCATION
	  ,D.RS_CODE 
       ,COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID,D.RS_CODE) AS CODE_NUM 
       ,COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID) AS TOTAL_CODE
       ,ROUND (((COUNT (D.RS_CODE) OVER (PARTITION BY  D.GLASS_ID,D.RS_CODE))::NUMERIC / (COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID)) ::NUMERIC ),3) AS CODE_RATIO
      ,P.PRODUCTCODE 
      ,('http://10.73.17.41/IMG_WEB/V3/'||D.RS_DEFECT_IMAGE_NAME ) AS ID
      ,T.PICI 
  FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
  LEFT JOIN DWR_MES_PRODUCTSPEC P
  ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
  ON D.GLASS_ID =H.CUT_ID 
  AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
  AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
 WHERE  H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
  AND D.GLASS_START_TIME >='${dtStartDate}'::timestamp
  AND D.GLASS_START_TIME <='${dtEndDate}'::timestamp
  ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
  ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
  ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
  ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  GROUP BY D.GLASS_START_TIME 
	      ,D.GLASS_ID 
	      ,H.SUB_EQUIP_ID 
	      ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14)  
	      ,PANEL_LOCATION 
	      ,D.RS_CODE 
	      ,P.PRODUCTCODE
	      ,D.RS_DEFECT_IMAGE_NAME
	      ,T.PICI 
	      )A
WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND A.PANEL_LOCATION IN ('"+cmcbPanelLocation+"')")}
  ORDER BY A.RS_CODE
          ,A.SUB_EQUIP_ID
          ,A.PRODUCTCODE
          ,A.GLASS_ID
```

## 补充语句
1. `SEARCH_BYBORDER`：
```
-- CODE BY边框
WITH TOTAL_CODE AS (
 SELECT COUNT (A.RS_CODE) AS TOTAL_CODE 
       ,A.PANEL_LOCATION
   FROM (
 SELECT 
        D.RS_CODE
       ,CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LT' THEN 'LEFTTOP '
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='T0' THEN 'TOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RT' THEN 'RIGHTTOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='R0' THEN 'RIGHT' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RB' THEN 'RIGHTBOTTOM' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9') THEN 'BOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LB' THEN 'LEFTBOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='L0' THEN 'LEFT'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
   LEFT JOIN DWR_MES_PRODUCTSPEC P 
          ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
   LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
  WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP 
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
    AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
    )A
WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND A.PANEL_LOCATION IN ('"+cmcbPanelLocation+"')")}
 GROUP BY A.PANEL_LOCATION
 ) 
 ,CODE_NUM AS (
 SELECT COUNT (A.RS_CODE) AS CODE_NUM
       ,COUNT(A.PANEL_ID) as PANEL_NUM
       ,A.RS_CODE
       ,A.PANEL_LOCATION
   FROM (
 SELECT D.RS_CODE 
       ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14) AS PANEL_ID
       ,CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LT' THEN 'LEFTTOP '
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='T0' THEN 'TOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RT' THEN 'RIGHTTOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='R0' THEN 'RIGHT' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RB' THEN 'RIGHTBOTTOM' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9') THEN 'BOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LB' THEN 'LEFTBOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='L0' THEN 'LEFT'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D 
  LEFT JOIN DWR_MES_PRODUCTSPEC P 
         ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC 
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
   WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP 
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
    AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  )A
WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND A.PANEL_LOCATION IN ('"+cmcbPanelLocation+"')")}
   GROUP BY A.RS_CODE
         ,A.PANEL_LOCATION
 )
SELECT A.PANEL_LOCATION
      ,B.RS_CODE
      ,ROUND (SUM (B.CODE_NUM) ::NUMERIC / SUM (A.TOTAL_CODE) ::NUMERIC,5) AS CODE_RATIO
      ,SUM(B.PANEL_NUM) ::numeric as PANEL_NUM
  FROM TOTAL_CODE A 
  LEFT JOIN CODE_NUM B 
         ON A.PANEL_LOCATION = B.PANEL_LOCATION
  WHERE B.RS_CODE IS NOT NULL
  GROUP BY B.RS_CODE
	      ,A.PANEL_LOCATION
  ORDER BY CASE WHEN A.PANEL_LOCATION = 'TOP' THEN 1
                WHEN A.PANEL_LOCATION = 'BOTTOM' THEN 2
                WHEN A.PANEL_LOCATION = 'LEFT' THEN 3
                WHEN A.PANEL_LOCATION = 'RIGHT' THEN 4
                WHEN A.PANEL_LOCATION = 'LEFTTOP' THEN 5
                WHEN A.PANEL_LOCATION = 'RIGHTTOP' THEN 6
                WHEN A.PANEL_LOCATION = 'LEFTBOTTOM' THEN 7
                WHEN A.PANEL_LOCATION = 'RIGHTBOTTOM' THEN 8
                END
          ,B.RS_CODE
```

8. `SEARCH_BYBORDER1`：
```
-- CODE BY边框
WITH TOTAL_CODE AS (
 SELECT COUNT (A.RS_CODE) AS TOTAL_CODE 
       ,A.PANEL_LOCATION
   FROM (
 SELECT 
        D.RS_CODE
       , CASE WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) ='LT'   THEN 'LEFTTOP'
           WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'L0' and 'L9'   THEN 'LEFT'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) between 'T0' and 'T9' THEN 'TOP'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) ='RT' THEN 'RIGHTTOP'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'R0' and 'R9'  THEN 'RIGHT' 
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) = 'RB'  THEN 'RIGHTBOTTOM' 
		      WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) = 'LB'  THEN 'LEFTBOTTOM' 
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'B0' and 'B9' 
		      THEN 'BOTTOM'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'HL0' and 'HL9'  THEN 'KONGLEFT'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3) between 'HT0' and 'HT9' THEN 'KONGTOP'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3)  between 'HR0' and 'HR9'  THEN 'KONGRIGHT' 
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3)  between 'HB0' and 'HB9' 
		      THEN 'KONGBOTTOM'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
   LEFT JOIN DWR_MES_PRODUCTSPEC P 
          ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
   LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
  WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2')
    AND H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  union all 
   SELECT 
        D.RS_CODE
       ,CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B0' THEN 'BOTTOM0'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B1' THEN 'BOTTOM1'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B2' THEN 'BOTTOM2'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B3' THEN 'BOTTOM3'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B4' THEN 'BOTTOM4'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B5' THEN 'BOTTOM5'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B6' THEN 'BOTTOM6'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B7' THEN 'BOTTOM7'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B8' THEN 'BOTTOM8'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B9' THEN 'BOTTOM9'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
   LEFT JOIN DWR_MES_PRODUCTSPEC P 
          ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
   LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
   LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
          ON H.ITEM5 = V.SUB_PROD_ID 
   LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
  WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP 
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND  H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
    and SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9')
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
    )A
 WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND CASE WHEN A.PANEL_LOCATION like 'BOTTOM%' THEN 'BOTTOM' ELSE A.PANEL_LOCATION END IN ('"+cmcbPanelLocation+"')")} 
 GROUP BY A.PANEL_LOCATION
 ) 
 ,CODE_NUM AS (
 SELECT COUNT (A.RS_CODE) AS CODE_NUM
       ,COUNT(A.PANEL_ID) as PANEL_NUM
       ,A.RS_CODE
       ,A.PANEL_LOCATION
   FROM (
 SELECT D.RS_CODE 
       ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14) AS PANEL_ID
        , CASE WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) ='LT'   THEN 'LEFTTOP'
           WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'L0' and 'L9'   THEN 'LEFT'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) between 'T0' and 'T9' THEN 'TOP'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) ='RT' THEN 'RIGHTTOP'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'R0' and 'R9'  THEN 'RIGHT' 
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) = 'RB'  THEN 'RIGHTBOTTOM' 
		      WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) = 'LB'  THEN 'LEFTBOTTOM' 
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'B0' and 'B9' 
		      THEN 'BOTTOM'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'HL0' and 'HL9'  THEN 'KONGLEFT'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3) between 'HT0' and 'HT9' THEN 'KONGTOP'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3)  between 'HR0' and 'HR9'  THEN 'KONGRIGHT' 
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3)  between 'HB0' and 'HB9' 
		      THEN 'KONGBOTTOM'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D 
  LEFT JOIN DWR_MES_PRODUCTSPEC P 
         ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC 
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
   LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
          ON H.ITEM5 = V.SUB_PROD_ID 
    LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
   WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP 
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  union all 
   SELECT D.RS_CODE 
       ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14) AS PANEL_ID
       ,CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B0' THEN 'BOTTOM0'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B1' THEN 'BOTTOM1'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B2' THEN 'BOTTOM2'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B3' THEN 'BOTTOM3'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B4' THEN 'BOTTOM4'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B5' THEN 'BOTTOM5'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B6' THEN 'BOTTOM6'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B7' THEN 'BOTTOM7'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B8' THEN 'BOTTOM8'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B9' THEN 'BOTTOM9'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D 
  LEFT JOIN DWR_MES_PRODUCTSPEC P 
         ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC 
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
   WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND  H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
    and SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9')
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  )A
WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND CASE WHEN A.PANEL_LOCATION like 'BOTTOM%' THEN 'BOTTOM' ELSE A.PANEL_LOCATION END IN ('"+cmcbPanelLocation+"')")} 
   GROUP BY A.RS_CODE
           ,A.PANEL_LOCATION
 )
SELECT A.PANEL_LOCATION
      ,B.RS_CODE
      ,ROUND (SUM (B.CODE_NUM) ::NUMERIC / SUM (A.TOTAL_CODE) ::NUMERIC,5) AS CODE_RATIO
      ,SUM(B.PANEL_NUM) ::numeric as PANEL_NUM
  FROM TOTAL_CODE A 
  LEFT JOIN CODE_NUM B 
         ON A.PANEL_LOCATION = B.PANEL_LOCATION
   where A.PANEL_LOCATION is not null
  GROUP BY B.RS_CODE
	      ,A.PANEL_LOCATION
  ORDER BY A.PANEL_LOCATION
          ,B.RS_CODE
```

9. `SERACH_BYDAY`：
```
-- CODE BY天
WITH TOTAL_CODE AS (
 SELECT COUNT (A.RS_CODE) AS TOTAL_CODE 
       ,A.DATE_TIMEKEY
       ,A.PRODUCTCODE
       ,A.EQP_ID
       ,A.SUB_EQUIP_ID
   FROM (
 SELECT 
        D.RS_CODE
       ,TO_CHAR (D.GLASS_START_TIME,'YYYYMMDD') AS DATE_TIMEKEY
       ,P.PRODUCTCODE
       ,SUBSTRING(H.SUB_EQUIP_ID,1,6) AS EQP_ID
       ,H.SUB_EQUIP_ID 
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
   LEFT JOIN DWR_MES_PRODUCTSPEC P 
          ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
   LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp- INTERVAL '7DAY'
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
  WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP - INTERVAL '7DAY'
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
    )A
 GROUP BY A.DATE_TIMEKEY
         ,A.PRODUCTCODE
         ,A.EQP_ID
         ,A.SUB_EQUIP_ID
 ) 
 ,CODE_NUM AS (
 SELECT COUNT (A.RS_CODE) AS CODE_NUM
       ,A.DATE_TIMEKEY
       ,A.RS_CODE
       ,A.PRODUCTCODE
       ,A.EQP_ID
       ,A.SUB_EQUIP_ID
   FROM (
 SELECT D.RS_CODE 
       ,TO_CHAR (D.GLASS_START_TIME,'YYYYMMDD') AS DATE_TIMEKEY
       ,P.PRODUCTCODE
       ,SUBSTRING(H.SUB_EQUIP_ID,1,6) AS EQP_ID
       ,H.SUB_EQUIP_ID 
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D 
  LEFT JOIN DWR_MES_PRODUCTSPEC P 
         ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC 
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp- INTERVAL '7DAY'
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
   WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP - INTERVAL '7DAY'
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND  H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  )A
   GROUP BY A.DATE_TIMEKEY
         ,A.RS_CODE
         ,A.PRODUCTCODE
         ,A.EQP_ID
         ,A.SUB_EQUIP_ID
 )
SELECT A.PRODUCTCODE
      ,A.EQP_ID
      ,A.SUB_EQUIP_ID
      ,SUBSTRING (A.DATE_TIMEKEY,5,2)||'-'||RIGHT (A.DATE_TIMEKEY,2) AS DATE_TIMEKEY
      ,B.RS_CODE
      ,ROUND (SUM (B.CODE_NUM) ::NUMERIC / SUM (A.TOTAL_CODE) ::NUMERIC,3) AS CODE_RATIO
  FROM TOTAL_CODE A 
  LEFT JOIN CODE_NUM B 
         ON A.DATE_TIMEKEY =B.DATE_TIMEKEY
        AND A.PRODUCTCODE =B.PRODUCTCODE
        AND A.EQP_ID =B.EQP_ID
        AND A.SUB_EQUIP_ID=B.SUB_EQUIP_ID
  GROUP BY A.DATE_TIMEKEY
          ,B.RS_CODE
	      ,A.PRODUCTCODE
	      ,A.EQP_ID
	      ,A.SUB_EQUIP_ID
  ORDER BY  B.RS_CODE  
           ,A.PRODUCTCODE
           ,A.SUB_EQUIP_ID 
           ,A.DATE_TIMEKEY
```

10. `SEARCH_SUBPRODTYPE`：
```
SELECT DISTINCT SUB_PROD_TYPE
FROM DWR_MES_PRODUCTREQUEST_V
WHERE FACTORY = 'OLED'
AND SUB_PROD_TYPE IS NOT NULL
ORDER BY SUB_PROD_TYPE
```

11. `SEARCH_TOTAL`：
```
-- Total图
 WITH TOTAL_CODE AS (
 SELECT COUNT (A.RS_CODE) AS TOTAL_CODE 
       ,A.PRODUCTCODE
       ,A.EQP_ID
       ,A.SUB_EQUIP_ID
   FROM (
 SELECT 
        D.RS_CODE
       ,TO_CHAR (D.GLASS_START_TIME,'YYYYMMDD') AS DATE_TIMEKEY
       ,P.PRODUCTCODE
       ,SUBSTRING(H.SUB_EQUIP_ID,1,6) AS EQP_ID
       ,H.SUB_EQUIP_ID 
       ,CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LT' THEN 'LEFTTOP '
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='T0' THEN 'TOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RT' THEN 'RIGHTTOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='R0' THEN 'RIGHT' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RB' THEN 'RIGHTBOTTOM' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9') THEN 'BOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LB' THEN 'LEFTBOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='L0' THEN 'LEFT'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
   LEFT JOIN DWR_MES_PRODUCTSPEC P 
          ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
   LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
  WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP 
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND  H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
    )A
WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND A.PANEL_LOCATION IN ('"+cmcbPanelLocation+"')")}
 GROUP BY A.PRODUCTCODE
         ,A.EQP_ID
         ,A.SUB_EQUIP_ID
 ) 
 ,CODE_NUM AS (
 SELECT COUNT (A.RS_CODE) AS CODE_NUM
       ,A.RS_CODE
       ,A.PRODUCTCODE
       ,A.EQP_ID
       ,A.SUB_EQUIP_ID
   FROM (
 SELECT D.RS_CODE 
       ,TO_CHAR (D.GLASS_START_TIME,'YYYYMMDD') AS DATE_TIMEKEY
       ,P.PRODUCTCODE
       ,SUBSTRING(H.SUB_EQUIP_ID,1,6) AS EQP_ID
       ,H.SUB_EQUIP_ID 
       ,CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LT' THEN 'LEFTTOP '
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='T0' THEN 'TOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RT' THEN 'RIGHTTOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='R0' THEN 'RIGHT' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RB' THEN 'RIGHTBOTTOM' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9') THEN 'BOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LB' THEN 'LEFTBOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='L0' THEN 'LEFT'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D 
  LEFT JOIN DWR_MES_PRODUCTSPEC P 
         ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC 
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
   WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP 
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND  H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
  ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  )A
WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND A.PANEL_LOCATION IN ('"+cmcbPanelLocation+"')")}
   GROUP BY A.RS_CODE
           ,A.PRODUCTCODE
           ,A.EQP_ID
           ,A.SUB_EQUIP_ID
 )
SELECT A.PRODUCTCODE
      ,A.EQP_ID
      ,A.SUB_EQUIP_ID 
      ,B.RS_CODE
      ,ROUND (SUM (B.CODE_NUM) ::NUMERIC / SUM (A.TOTAL_CODE) ::NUMERIC,3) AS CODE_RATIO
  FROM TOTAL_CODE A 
  LEFT JOIN CODE_NUM B 
         ON A.PRODUCTCODE =B.PRODUCTCODE
        AND A.EQP_ID =B.EQP_ID
        AND A.SUB_EQUIP_ID=B.SUB_EQUIP_ID
  GROUP BY B.RS_CODE
	      ,A.PRODUCTCODE
	      ,A.EQP_ID
	      ,A.SUB_EQUIP_ID
  ORDER BY A.SUB_EQUIP_ID 
          ,A.PRODUCTCODE
	     ,B.RS_CODE
```

12. `SERACH_TOTAL2`：
```
-- 新增Total图By产品 (YouYue 20250702)
 WITH TOTAL_CODE AS (
 SELECT COUNT (A.RS_CODE) AS TOTAL_CODE 
       ,A.PRODUCTCODE
   FROM (
 SELECT 
        D.RS_CODE
       ,TO_CHAR (D.GLASS_START_TIME,'YYYYMMDD') AS DATE_TIMEKEY
       ,P.PRODUCTCODE
       ,SUBSTRING(H.SUB_EQUIP_ID,1,6) AS EQP_ID
       ,H.SUB_EQUIP_ID 
       ,CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LT' THEN 'LEFTTOP '
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='T0' THEN 'TOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RT' THEN 'RIGHTTOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='R0' THEN 'RIGHT' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RB' THEN 'RIGHTBOTTOM' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9') THEN 'BOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LB' THEN 'LEFTBOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='L0' THEN 'LEFT'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D 
   LEFT JOIN DWR_MES_PRODUCTSPEC P 
          ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
   LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
  WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP 
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2')
    AND H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
   ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
    )A
 WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND A.PANEL_LOCATION IN ('"+cmcbPanelLocation+"')")}
 GROUP BY A.PRODUCTCODE
 ) 
 ,CODE_NUM AS (
 SELECT COUNT (A.RS_CODE) AS CODE_NUM
       ,A.RS_CODE
       ,A.PRODUCTCODE
   FROM (
 SELECT D.RS_CODE 
       ,TO_CHAR (D.GLASS_START_TIME,'YYYYMMDD') AS DATE_TIMEKEY
       ,P.PRODUCTCODE
       ,SUBSTRING(H.SUB_EQUIP_ID,1,6) AS EQP_ID
       ,H.SUB_EQUIP_ID 
       ,CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LT' THEN 'LEFTTOP '
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='T0' THEN 'TOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RT' THEN 'RIGHTTOP'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='R0' THEN 'RIGHT' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='RB' THEN 'RIGHTBOTTOM' 
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9') THEN 'BOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='LB' THEN 'LEFTBOTTOM'
		     WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='L0' THEN 'LEFT'
		     ELSE NULL END AS PANEL_LOCATION
   FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D 
  LEFT JOIN DWR_MES_PRODUCTSPEC P 
         ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC 
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
  LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
   WHERE D.GLASS_START_TIME >= '${dtStartDate}'::TIMESTAMP 
    AND D.GLASS_START_TIME <= '${dtEndDate}'::TIMESTAMP 
    AND H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
    AND RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4','C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2')
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
   ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  )A
WHERE 1=1
 ${IF(LEN(cmcbPanelLocation)=0,"","AND A.PANEL_LOCATION IN ('"+cmcbPanelLocation+"')")}
   GROUP BY A.RS_CODE
           ,A.PRODUCTCODE
 )
SELECT A.PRODUCTCODE
      ,B.RS_CODE
      ,B.CODE_NUM
      ,ROUND (SUM (B.CODE_NUM) ::NUMERIC / SUM (A.TOTAL_CODE) ::NUMERIC,3) AS CODE_RATIO
  FROM TOTAL_CODE A 
  LEFT JOIN CODE_NUM B 
         ON A.PRODUCTCODE =B.PRODUCTCODE
  GROUP BY B.RS_CODE
	      ,A.PRODUCTCODE
	      ,B.CODE_NUM
  ORDER BY A.PRODUCTCODE
	     ,B.RS_CODE
```

13. `SERACH1`：
```
select GLASS_START_TIME
      ,GLASS_ID
      ,SUB_EQUIP_ID
      ,PANEL_ID
      ,PANEL_LOCATION
      ,RS_CODE
      ,CODE_NUM
      ,TOTAL_CODE
      ,CODE_RATIO
      ,PRODUCTCODE
      ,ID
from 
(SELECT D.GLASS_START_TIME 
      ,D.GLASS_ID
      ,H.SUB_EQUIP_ID
      ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14) AS PANEL_ID 
	 , CASE WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) ='LT'   THEN 'LEFTTOP'
           WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'L0' and 'L9'   THEN 'LEFT'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) between 'T0' and 'T9' THEN 'TOP'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) ='RT' THEN 'RIGHTTOP'
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'R0' and 'R9'  THEN 'RIGHT' 
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) = 'RB'  THEN 'RIGHTBOTTOM' 
		      WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2) = 'LB'  THEN 'LEFTBOTTOM' 
		     WHEN D.RS_CODE like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'B0' and 'B9' 
		      THEN 'BOTTOM'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),2)  between 'HL0' and 'HL9'  THEN 'KONGLEFT'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3) between 'HT0' and 'HT9' THEN 'KONGTOP'
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3)  between 'HR0' and 'HR9'  THEN 'KONGRIGHT' 
		     WHEN D.RS_CODE not like 'C3DM%' and right(split_part(split_part(D.RS_DEFECT_IMAGE_NAME,'/',10),'_',1),3)  between 'HB0' and 'HB9' 
		      THEN 'KONGBOTTOM'
		     ELSE NULL END AS PANEL_LOCATION
		     
	  ,D.RS_CODE 
       ,COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID,D.RS_CODE) AS CODE_NUM 
       ,COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID) AS TOTAL_CODE
       ,ROUND (((COUNT (D.RS_CODE) OVER (PARTITION BY  D.GLASS_ID,D.RS_CODE))::NUMERIC / (COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID)) ::NUMERIC ),3) AS CODE_RATIO
      ,P.PRODUCTCODE 
      ,('http://10.73.17.41/IMG_WEB/V3/'||D.RS_DEFECT_IMAGE_NAME ) AS ID
  FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
  LEFT JOIN DWR_MES_PRODUCTSPEC P
  ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
   LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
 WHERE  H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
  AND D.GLASS_START_TIME >='${dtStartDate}'::timestamp
  AND D.GLASS_START_TIME <='${dtEndDate}'::timestamp
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
   ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  GROUP BY D.GLASS_START_TIME 
	      ,D.GLASS_ID 
	      ,H.SUB_EQUIP_ID 
	      ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14)  
	      ,PANEL_LOCATION 
	      ,D.RS_CODE 
	      ,P.PRODUCTCODE
	      ,D.RS_DEFECT_IMAGE_NAME
union all
SELECT D.GLASS_START_TIME 
      ,D.GLASS_ID
      ,H.SUB_EQUIP_ID
      ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14) AS PANEL_ID 
	  ,  CASE WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B0' THEN 'BOTTOM0'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B1' THEN 'BOTTOM1'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B2' THEN 'BOTTOM2'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B3' THEN 'BOTTOM3'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B4' THEN 'BOTTOM4'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B5' THEN 'BOTTOM5'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B6' THEN 'BOTTOM6'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B7' THEN 'BOTTOM7'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B8' THEN 'BOTTOM8'
             WHEN SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) ='B9' THEN 'BOTTOM9'
		     ELSE NULL END AS PANEL_LOCATION
	  ,D.RS_CODE 
       ,COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID,D.RS_CODE) AS CODE_NUM 
       ,COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID) AS TOTAL_CODE
       ,ROUND (((COUNT (D.RS_CODE) OVER (PARTITION BY  D.GLASS_ID,D.RS_CODE))::NUMERIC / (COUNT (D.RS_CODE) OVER (PARTITION BY D.GLASS_ID)) ::NUMERIC ),3) AS CODE_RATIO
      ,P.PRODUCTCODE 
      ,('http://10.73.17.41/IMG_WEB/V3/'||D.RS_DEFECT_IMAGE_NAME ) AS ID
  FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D
  LEFT JOIN DWR_MES_PRODUCTSPEC P
  ON P.PRODUCTSPECNAME =D.PRODUCT_SPEC  
  LEFT JOIN EDA.OLED_CHAMBER_HST_T H
		  ON D.GLASS_ID =H.CUT_ID 
		 AND H.CUT_START_TIME >= '${dtStartDate}'::timestamp
		 AND H.CUT_START_TIME <='${dtEndDate}'::timestamp
  LEFT JOIN DWR_MES_PRODUCTREQUEST_V V 
         ON H.ITEM5 = V.SUB_PROD_ID 
   LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T 
         ON D.GLASS_ID =T.GLASS_ID 
 WHERE   H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2','3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')
  AND D.RS_CODE IN ('C3DM0', 'C3DM1', 'C3DM2', 'C3DM3', 'C3DM4', 'C3DM5','C3RA1','C3RA2','C3RA3','C3ZC1','C3BH1','C3BH2') 
  AND D.GLASS_START_TIME >='${dtStartDate}'::timestamp
  AND D.GLASS_START_TIME <='${dtEndDate}'::timestamp
  and SUBSTRING(D.RS_DEFECT_IMAGE_NAME,71, 2) IN ('B0','B1','B2','B3','B4','B5','B6','B7','B8','B9')
 ${IF(LEN(cmcbProduct)=0,"","AND P.productspecname IN ('"+cmcbProduct+"')")}
 ${IF(LEN(cmcbProdCode)=0,"","AND P.productcode IN ('"+cmcbProdCode+"')")}
 ${IF(LEN(cmcbLine)=0,"","AND substring(H.sub_equip_id,1,6) IN ('"+cmcbLine+"')")}
 ${IF(LEN(cmcbSubProdID)=0,"","AND H.sub_equip_id IN ('"+cmcbSubProdID+"')")}
  ${IF(LEN(txtGlass)=0,"","AND D.GLASS_ID IN ('"+txtGlass+"')")}
  ${IF(LEN(cmcbSubProdType)=0,"","AND V.sub_prod_type IN ('"+cmcbSubProdType+"')")}
  ${IF(LEN(cmcbCode)=0,"","AND D.RS_CODE IN ('"+cmcbCode+"')")}
   ${IF(LEN(cmcbLot)=0,"","AND T.PICI IN ('"+cmcbLot+"')")}
  ${IF(LEN(cmcbCycle)=0,"","AND T.CYCLE_ID IN ('"+cmcbCycle+"')")}
  GROUP BY D.GLASS_START_TIME 
	      ,D.GLASS_ID 
	      ,H.SUB_EQUIP_ID 
	      ,SUBSTRING(D.RS_DEFECT_IMAGE_NAME,57, 14)  
	      ,PANEL_LOCATION 
	      ,D.RS_CODE 
	      ,P.PRODUCTCODE
	      ,D.RS_DEFECT_IMAGE_NAME
)A
WHERE 1=1
${IF(LEN(cmcbPanelLocation)=0,"","AND CASE WHEN A.PANEL_LOCATION like 'BOTTOM%' THEN 'BOTTOM' ELSE A.PANEL_LOCATION END IN ('"+cmcbPanelLocation+"')")} 
  ORDER BY 
          PRODUCTCODE
          ,GLASS_START_TIME
          ,RS_CODE
          ,GLASS_ID
          ,SUB_EQUIP_ID 
```