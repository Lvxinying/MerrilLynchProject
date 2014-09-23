package MarketPriceBackFeedToTSDB;

//存储对应的BlobType的类名和TsType的整数码
public enum TsBlobDataTypeBaseObject {
//DataPoint(MarketPrice for Equity, ETF) 	
	tsMarketPrice("BlobDLLLL",2),
	tsMarketPriceCurrencyHistory("BlobDCj",1584),
	tsMarketPriceCopyOver("BlobDDB",3222),
	tsMarketBidOfferMidPrice("BlobDLLL", 2146),
	tsTradingVolume("BlobDL",98),	
//DataPoint(Warrant MarketPrice)	
	tsWarrantMarketPrice("BlobDLLLL",3234),
	tsWarrantMarketPriceCurrencyHistory("BlobDCj",3236),
	tsWarrantTradingVolume("BlobDL",3237),
//DataPoint(Extra Price)
	tsCalculatedTurnOver("BlobDL",2474),
	tsTradeCount("BlobDL",2475),
	tsExchangeTurnOver("BlobDL",2476),
	tsBidOfferSpread("BlobDLLLLL",2541),
//DataPoint(Exchange Rate)	
	tsTenforeCurrencyTradingPrice("BlobDLLLL",2121),
	tsForexSpotExchangeRate("BlobDL",1575),
//DataPoint(Non Trade Dates)	
	tsNonTradingDate("BlobDB",2030),
//DataPoint(Price Corporate Action)	
	tsShareSplitRatio("BlobDLL",2470),
	tsSpinoff("BlobDBSet50",101),
	tsStockDistribution("BlobDFF",1115),
	tsSpecialCashDividend("BlobDL",1127),
	tsDividendFrequency("BlobDB",1211),
	tsCashDividend("BlobDL",1578),
	tsCashDividendDates("BlobDDDD",1579),
	tsCashDividendCurrencyHistory("BlobDCj",1580),
	tsSpecialCashDividendDates("BlobDDDD",1581),
	tsSpecialCashDividendCurrencyHistory("BlobDCj",1582),
	tsSplitDates("BlobDDDD",2493),
	tsStockDistributionDates("BlobDDDD",2494),
	tsRightsOfferingDates("BlobDDDD",2508),
	tsRightsOffering("BlobDLLL",2510),
	tsRightsOfferingAdjustmentFactor("BlobDL",2511),
	tsRightsOfferingCurrencyHistory("BlobDCj",2512),
//DataPoint(Daily Market Return Index)
	tsDailyMarketReturnIndex("BlobDLB",16),
//DataPoint(Monthly Return for Equity, ETF, Index, and Exchange Rate)
	tsMonthlyMarketReturn("BlobDF",6),
//DataPoint(Quarterly Return for Equity, ETF, Index, and Exchange Rate)	
	tsQuarterlyMarketReturn("BlobDF",9),
//DataPoint(Annual Return for Equity, ETF, Index, and Exchange Rate)	
	tsAnnualMarketReturn("BlobDF",12),	
//DataPoint(Raw Capital Gain for ETF)	
	tsCapitalGain("BlobDDF",5)
	;

	public String BlobClassName;
	public int TsType;

	private TsBlobDataTypeBaseObject(String className, int tsType) {
		this.BlobClassName = className;
		this.TsType = tsType;
	}	
}
