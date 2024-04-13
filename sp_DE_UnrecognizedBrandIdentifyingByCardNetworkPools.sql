USE [DE_Workspace]
GO
/****** Object:  StoredProcedure [DWH\skacar].[sp_DE_UnrecognizedBrandIdentifyingByCardNetworkPools]    Script Date: 4/8/2024 2:11:38 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [DWH\skacar].[sp_DE_UnrecognizedBrandIdentifyingByCardNetworkPools] AS
DROP TABLE IF EXISTS DE_Workspace.[DWH\skacar].ATable,DE_Workspace.[DWH\skacar].BTable,DE_Workspace.[DWH\skacar].CTable, DE_Workspace.[DWH\skacar].dTable
SELECT
	 L.CardNetworkPoolId
	,L.MerchantCategoryCodeId
	,L.PosCategoriesId
	,L.RecognizedBrandId
	,L.CardAcceptorName
	,L.[CardAcceptorDescription]
	,LD.City
	,PDC.CityCodeTR
	,MAX(l.TransactionDateTime) MaxTransactionDateTimeAll
	,COUNT(L.Id)	  TxCountAll
INTO		DE_Workspace.[DWH\skacar].ATable	
FROM		DWH_DWH.dbo.FACT_Transactions			 L with (nolock)
JOIN		DWH_DWH.dbo.FACT_Transactions_Details  LD with (Nolock) ON L.Id = LD.Id
LEFT JOIN   DWH_DWH.dbo.DIM_DEDWHCardDomesticCities pdc with (Nolock) on LD.City = PDC.City
WHERE RecognizedBrandId IS NOT NULL AND IsPyshicalPOSTransaction = 1 AND IsNotDomestic = 0 AND Cancelled = 0 AND OperationEmployeeKey IS NULL
GROUP BY 	 L.CardNetworkPoolId
			,L.MerchantCategoryCodeId
			,L.PosCategoriesId
			,L.RecognizedBrandId
			,L.CardAcceptorName
			,L.[CardAcceptorDescription]
			,LD.City
			,PDC.CityCodeTR
SELECT
	 L.CardNetworkPoolId
	,L.MerchantCategoryCodeId
	,L.PosCategoriesId
	,A.RecognizedBrandId PossibleRecognizedBrandId
	,L.CardAcceptorName
	,L.[CardAcceptorDescription]
	,LD.City
	,PDC.CityCodeTR
	,MAX(l.TransactionDateTime) MaxTransactionDateTimeAll
	,COUNT(L.Id)	  TxCountAll
	,MAX(MAX(l.TransactionDateTime))	 OVER (PARTITION BY l.CardNetworkPoolId) MaxTransactionDateTimeCardNetworkPoolId
	,SUM(COUNT(L.Id))		 OVER (PARTITION BY l.CardNetworkPoolId) TxCountByCardNetworkPoolId
INTO		DE_Workspace.[DWH\skacar].BTable	
FROM		DWH_DWH.dbo.FACT_Transactions			 L with (nolock)
JOIN		DWH_DWH.dbo.FACT_Transactions_Details  LD with (Nolock) ON L.Id = LD.Id
JOIN		DE_Workspace.[DWH\skacar].ATable								 a				 ON L.CardNetworkPoolId = A.CardNetworkPoolId
LEFT JOIN   DWH_DWH.dbo.DIM_DEDWHCardDomesticCities pdc with (Nolock) on LD.City = PDC.City
WHERE L.RecognizedBrandId IS NULL AND IsPyshicalPOSTransaction = 1 AND IsNotDomestic = 0 AND Cancelled = 0 AND OperationEmployeeKey IS NULL
GROUP BY 	 L.CardNetworkPoolId
			,L.MerchantCategoryCodeId
			,L.PosCategoriesId
			,A.RecognizedBrandId
			,L.CardAcceptorName
			,L.[CardAcceptorDescription]
			,LD.City
			,PDC.CityCodeTR
select
	 ISNULL(A.CardNetworkPoolId,B.CardNetworkPoolId) CardNetworkPoolId
	,ISNULL(B.MerchantCategoryCodeId,A.MerchantCategoryCodeId) MerchantCategoryCodeId
	,ISNULL(B.PosCategoriesId,A.PosCategoriesId) PosCategoriesId
	,ISNULL(a.RecognizedBrandId,B.PossibleRecognizedBrandId) RecognizedBrandId
	,ISNULL(A.City,B.City) City
	,ISNULL(A.CityCodeTR,B.CityCodeTR) CityCodeTr
	,ISNULL(A.CardAcceptorName,B.CardAcceptorName) CardAcceptorName
	,ISNULL(A.[CardAcceptorDescription],B.[CardAcceptorDescription])		[CardAcceptorDescription]
	,IIF(A.MaxTransactionDateTimeAll >= B.MaxTransactionDateTimeAll,1,0) RecognizedBrandTxDateDEggerThanUnrecognizedOne
	,IIF(a.CardAcceptorName = b.CardAcceptorName AND A.City = B.City AND a.CardNetworkPoolId = B.CardNetworkPoolId AND A.[CardAcceptorDescription] = B.[CardAcceptorDescription] AND A.PosCategoriesId = B.PosCategoriesId AND A.PosCategoriesId = B.PosCategoriesId,1,0)
	IsRecognizedAfterAll
	,B.TxCountAll UnrecognizedCount
	,B.TxCountByCardNetworkPoolId UnrecognizedCountByCardNetworkPoolId
	,b.MaxTransactionDateTimeAll MaxTransactionDateTimeAllUnreconized
INTO DE_Workspace.[DWH\skacar].CTable
from DE_Workspace.[DWH\skacar].BTable b
FULL OUTER JOIN DE_Workspace.[DWH\skacar].ATable a on 
					   a.CardNetworkPoolId = B.CardNetworkPoolId
				   AND a.RecognizedBrandId = b.PossibleRecognizedBrandId
				   AND a.CardAcceptorName = b.CardAcceptorName
				   AND A.City = B.City
				   AND A.CityCodeTR = b.CityCodeTR
				   AND A.[CardAcceptorDescription] = B.[CardAcceptorDescription]
				   AND A.PosCategoriesId = B.PosCategoriesId
				   AND A.MerchantCategoryCodeId = B.MerchantCategoryCodeId
select eca.BrandName,t.*,IIF(K.CityCodeTr = T.CityCodeTr,1,0) SameCity 
into DE_Workspace.[DWH\skacar].dTable
from DE_Workspace.[DWH\skacar].CTable t
join (select distinct CardNetworkPoolId,citycodetr from DE_Workspace.[DWH\skacar].ATable) K ON T.CityCodeTr = k.CityCodeTR and t.CardNetworkPoolId = K.CardNetworkPoolId
join dwh_DWH.dbo.DIM_RecognizedBrands eca on t.RecognizedBrandId = eca.Id
where IsRecognizedAfterAll=0
TRUNCATE TABLE DE_Workspace.[DWH\skacar].FACT_DE_UnrecognizedBrandIdentifyingByCardNetworkPools
INSERT INTO	   DE_Workspace.[DWH\skacar].FACT_DE_UnrecognizedBrandIdentifyingByCardNetworkPools
select CardNetworkPoolId
,UnrecognizedCountByCardNetworkPoolId
,CASE WHEN										 UnrecognizedCountByCardNetworkPoolId >= 1000000 THEN 0
	  WHEN UnrecognizedCountByCardNetworkPoolId>= 500000 AND UnrecognizedCountByCardNetworkPoolId <  1000000 THEN 1
	  WHEN UnrecognizedCountByCardNetworkPoolId>= 250000 AND UnrecognizedCountByCardNetworkPoolId <  500000  THEN 2
	  WHEN UnrecognizedCountByCardNetworkPoolId>= 100000 AND UnrecognizedCountByCardNetworkPoolId <  250000  THEN 3
	  WHEN UnrecognizedCountByCardNetworkPoolId>= 50000  AND UnrecognizedCountByCardNetworkPoolId <  100000  THEN 4
	  WHEN UnrecognizedCountByCardNetworkPoolId>= 10000  AND UnrecognizedCountByCardNetworkPoolId <  50000   THEN 5
	  WHEN UnrecognizedCountByCardNetworkPoolId>= 5000   AND UnrecognizedCountByCardNetworkPoolId <  10000   THEN 6
	  WHEN UnrecognizedCountByCardNetworkPoolId>= 1000   AND UnrecognizedCountByCardNetworkPoolId <  5000    THEN 7
	  WHEN UnrecognizedCountByCardNetworkPoolId>= 1		 AND UnrecognizedCountByCardNetworkPoolId <  1000    THEN 8
 END UnrecognizedBrandsTxCountMagnitudeIntervalsByCardNetworkPoolId
,MerchantCategoryCodeId
,PosCategoriesId
,d.RecognizedBrandId
,CityCodeTr
,CardAcceptorName
,d.[CardAcceptorDescription]
,UnrecognizedCount UnrecognizedTxCountEverOnDomestic
,MaxTransactionDateTimeAllUnreconized
,FORMAT(MaxTransactionDateTimeAllUnreconized,'yyyyMM') MonthKeyOfLastUnrecognized
,IIF(d.RecognizedBrandId=471,1,0)		IsPharmacyTag
,IIF(d.RecognizedBrandId=491,1,0)		IsMarketTag
,IIF(d.RecognizedBrandId=13741,1,0)   IsTobaccoTag
,IIF(d.RecognizedBrandId IN (10061,24401,24421,24441,24461,24471,24501,24571,24581),1,0) IsCivilTransportationTag
,IIF(CardNetworkPoolId IN ('00000000','99999999','21212121'),1,0)		   IsNonsenseCardNetworkPoolId
,IIF(CB.RecognizedBrandId IS NOT NULL,1,0)				   IsCashbackCampaignBrand
,SumTxCountEver RecognizedTxCountEverOnDomestic
,CASE WHEN											  DomesticBrands.SumTxCountEver >= 1000000 THEN 0
	  WHEN DomesticBrands.SumTxCountEver>= 500000 AND DomesticBrands.SumTxCountEver <  1000000 THEN 1
	  WHEN DomesticBrands.SumTxCountEver>= 250000 AND DomesticBrands.SumTxCountEver <  500000  THEN 2
	  WHEN DomesticBrands.SumTxCountEver>= 100000 AND DomesticBrands.SumTxCountEver <  250000  THEN 3
	  WHEN DomesticBrands.SumTxCountEver>= 50000  AND DomesticBrands.SumTxCountEver <  100000  THEN 4
	  WHEN DomesticBrands.SumTxCountEver>= 10000  AND DomesticBrands.SumTxCountEver <  50000   THEN 5
	  WHEN DomesticBrands.SumTxCountEver>= 5000   AND DomesticBrands.SumTxCountEver <  10000   THEN 6
	  WHEN DomesticBrands.SumTxCountEver>= 1000   AND DomesticBrands.SumTxCountEver <  5000    THEN 7
	  WHEN DomesticBrands.SumTxCountEver>= 1	  AND DomesticBrands.SumTxCountEver <  1000    THEN 8
 END RecognizedBrandsTxCountMagnitudeIntervals
,CASE WHEN								  UnrecognizedCount >= 1000000 THEN 0
	  WHEN UnrecognizedCount>= 500000 AND UnrecognizedCount <  1000000 THEN 1
	  WHEN UnrecognizedCount>= 250000 AND UnrecognizedCount <  500000  THEN 2
	  WHEN UnrecognizedCount>= 100000 AND UnrecognizedCount <  250000  THEN 3
	  WHEN UnrecognizedCount>= 50000  AND UnrecognizedCount <  100000  THEN 4
	  WHEN UnrecognizedCount>= 10000  AND UnrecognizedCount <  50000   THEN 5
	  WHEN UnrecognizedCount>= 5000   AND UnrecognizedCount <  10000   THEN 6
	  WHEN UnrecognizedCount>= 1000   AND UnrecognizedCount <  5000    THEN 7
	  WHEN UnrecognizedCount>= 1	  AND UnrecognizedCount <  1000    THEN 8
 END UnrecognizedBrandsTxCountMagnitudeIntervals
from DE_Workspace.[DWH\skacar].dTable d
left join (
		   select Distinct RecognizedBrandId
		   from dwh_DWH.dbo.DIM_CashbackConditions
		   where ApplicableToEntryType=2 
		   	 and IsTravelReward=0
		   	 and RecognizedBrandId is not null
		   	 and cast(getdate() as date)<=EndDate
		  ) CB ON D.RecognizedBrandId = CB.RecognizedBrandId
left join (
			SELECT 
			 RecognizedBrandId
			,SUM(TxCountDaily) SumTxCountEver
			FROM DWH_CUSTOMTABLES.DBO.FACT_ExternalCardAcceptorCitiesMTDCube  ec
			where CityCodeTR=10000 AND RecognizedBrandId not in (10000,-80)
			group by RecognizedBrandId
		  ) DomesticBrands ON DomesticBrands.RecognizedBrandId = D.RecognizedBrandId
DROP TABLE IF EXISTS DE_Workspace.[DWH\skacar].ATable,DE_Workspace.[DWH\skacar].BTable,DE_Workspace.[DWH\skacar].CTable, DE_Workspace.[DWH\skacar].dTable