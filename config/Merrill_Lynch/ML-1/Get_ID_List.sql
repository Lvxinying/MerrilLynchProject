INSERT INTO Temp.TempInvestmentIdDimension
(InvestmentKey,InvestmentId, VendorId, InvestmentName, CreateDateTime, LastUpdateDateTime)
AT EPOCH LATEST 
SELECT i.InvestmentKey,i.InvestmentId, i.VendorId, i.InvestmentName, i.CreateDateTime, i.LastUpdateDateTime
 FROM dbo.InvestmentIdDimension i
	INNER JOIN dbo.FixedIncome f ON f.InvestmentKey = i.InvestmentKey
	INNER JOIN  (
	   SELECT distinct(d.InvestmentId)
	   FROM dbo.PerformanceIdDimension d 
	   INNER JOIN dbo.BondPriceHistory pf on pf.PerformanceKey = d.PerformanceKey
	   WHERE d.VendorId = 101
	) p ON p.InvestmentId = i.InvestmentId
	WHERE i.VendorId = 101
	ORDER BY i.InvestmentId ASC;
