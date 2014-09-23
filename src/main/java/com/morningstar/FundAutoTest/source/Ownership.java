package com.morningstar.FundAutoTest.source;

import java.io.InputStream;
import java.util.HashMap;

import com.morningstar.FundAutoTest.HttpConnection;
import com.morningstar.FundAutoTest.XmlHelper;

public class Ownership {

	private static String ownership_url = "http://ownership.morningstar.com/ownershipdata/api/GetData.aspx?function=GetOwnershipData&cusip=%s&ownertype=mutualfund";
	private static String owner = "/OwnershipData/Owners/Owner";
	
	public static HashMap<String, String> getData(String CUSIP, String ownerName){
		HashMap<String, String> map = new HashMap<String, String>();
		
		InputStream in = HttpConnection.getInputStream(String.format(ownership_url, CUSIP));
		XmlHelper.readStream(in);
		
		int num = XmlHelper.getNodeCount(owner);
		for(int i=1;i<num;i++){
			String AsOfDate = "/OwnershipData/Owners/Owner["+i+"]/AsOfDate";
			String OwnerName = "/OwnershipData/Owners/Owner["+i+"]/OwnerName";
			String ShareClassName = "/OwnershipData/Owners/Owner["+i+"]/ShareClassName";
			String OwnerType = "/OwnershipData/Owners/Owner["+i+"]/OwnerType";
			String CountryId = "/OwnershipData/Owners/Owner["+i+"]/CountryId";
			String Ticker = "/OwnershipData/Owners/Owner["+i+"]/Ticker";
			String SecId = "/OwnershipData/Owners/Owner["+i+"]/SecId";
			String NumberOfShares = "/OwnershipData/Owners/Owner["+i+"]/NumberOfShares";
			String MarketValue = "/OwnershipData/Owners/Owner["+i+"]/MarketValue";
			String ShareChange = "/OwnershipData/Owners/Owner["+i+"]/ShareChange";
			String PercentageInPortfolio = "/OwnershipData/Owners/Owner["+i+"]/PercentageInPortfolio";
			String PercentageOwnership = "/OwnershipData/Owners/Owner["+i+"]/PercentageOwnership";
			String Universe = "/OwnershipData/Owners/Owner["+i+"]/Universe";
			String PercentChangeFromPriorPort = "/OwnershipData/Owners/Owner["+i+"]/PercentChangeFromPriorPort";
			String PreviousPortfolioDate = "/OwnershipData/Owners/Owner["+i+"]/PreviousPortfolioDate";
			
			if(ownerName.equals(XmlHelper.getValueFromInputStream(OwnerName))){
				map.put("AsOfDate", XmlHelper.getValueFromInputStream(AsOfDate));
				map.put("ShareClassName", XmlHelper.getValueFromInputStream(ShareClassName));
				map.put("OwnerType", XmlHelper.getValueFromInputStream(OwnerType));
				map.put("CountryId", XmlHelper.getValueFromInputStream(CountryId));
				map.put("Ticker", XmlHelper.getValueFromInputStream(Ticker));
				map.put("SecId", XmlHelper.getValueFromInputStream(SecId));
				map.put("NumberOfShares", XmlHelper.getValueFromInputStream(NumberOfShares));
				map.put("MarketValue", XmlHelper.getValueFromInputStream(MarketValue));
				map.put("ShareChange", XmlHelper.getValueFromInputStream(ShareChange));
				map.put("PercentageInPortfolio", XmlHelper.getValueFromInputStream(PercentageInPortfolio));
				map.put("PercentageOwnership", XmlHelper.getValueFromInputStream(PercentageOwnership));
				map.put("Universe", XmlHelper.getValueFromInputStream(Universe));
				map.put("PercentChangeFromPriorPort", XmlHelper.getValueFromInputStream(PercentChangeFromPriorPort));
				map.put("PreviousPortfolioDate", XmlHelper.getValueFromInputStream(PreviousPortfolioDate));
			}
				
		}
		return map;
		
	}
}
