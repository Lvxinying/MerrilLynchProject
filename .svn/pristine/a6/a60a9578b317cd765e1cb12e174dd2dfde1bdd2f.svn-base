SELECT * FROM 
(
      SELECT VerticaDriAndReturn
      (
          perfIdDim.PerformanceKey, 
          perfIdDim.PerformanceId, 
          InvIdDim.InvestmentId, 
          priceFact.DateId, 
          priceFact.EvaluatedPrice,
          priceFact.Coupon, 
          fixedInc.CouponFrequency, 
          fixedInc.CouponType, 
          fixedInc.Coupon,
          fixedInc.DayCount,
          fixedInc.FirstCouponDateId, 
          fixedInc.NextCouponPaymentDateId, 
          fixedInc.MaturityDateId
      )
      OVER (PARTITION BY perfIdDim.PerformanceId ORDER BY perfIdDim.PerformanceId,priceFact.DateId) 
      FROM dbo.BondPriceHistory priceFact
      INNER JOIN (SELECT PerformanceKey, PerformanceId, InvestmentId, VendorId 
                  FROM dbo.PerformanceIdDimension 
                  WHERE VendorId = 101
                 ) perfIdDim ON priceFact.PerformanceKey = perfIdDim.PerformanceKey
      INNER JOIN ( SELECT InvestmentKey, InvestmentId, VendorId 
                   FROM Temp.TempInvestmentIdDimension
                   ORDER BY InvestmentId ASC
                   OFFSET 10 LIMIT 10
                  ) InvIdDim  ON InvIdDim.InvestmentId = perfIdDim.InvestmentId and InvIdDim.VendorId = perfIdDim.VendorId
      INNER JOIN dbo.FixedIncome fixedInc  ON fixedInc.InvestmentKey = InvIdDim.InvestmentKey
      WHERE priceFact.EvaluatedPrice > 1
) t 
WHERE MonthlyHistoricalReturn is not null
	ORDER BY t.InvestmentId, t.TradeDateId DESC;