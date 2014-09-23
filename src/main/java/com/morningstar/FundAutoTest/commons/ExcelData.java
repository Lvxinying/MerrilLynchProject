package com.morningstar.FundAutoTest.commons;

import java.util.ArrayList;
import java.util.List;

public class ExcelData {	
	static final String UNIVERSE_PATH = "config/universe.xlsx";
	
	static final String STOCK_SHEETNAME = "stock";
	static final int STOCK_EXCHANGEID = 0;
	static final int STOCK_IDENTIFIER = 1;
	static final int STOCK_SHARECLASSID = 2;
	
	static final String INDUSTRY_SHEETNAME = "industry";
	static final int INDUSTRY = 0;
	static final int INDUSTRY_GROUP = 1;
	static final int INDUSTRY_SECTOR = 2;
	
	public static List<List<String>> getUniverseStock() throws Exception
	{
		return ExcelUtil.getCellData(UNIVERSE_PATH, STOCK_SHEETNAME, true);
	}
	
	public static List<List<String>> getUniverseIndustry() throws Exception
	{
		int[] array_industry = { INDUSTRY, INDUSTRY_GROUP, INDUSTRY_SECTOR };
		return ExcelUtil.getColumnData(UNIVERSE_PATH, INDUSTRY_SHEETNAME, array_industry, true);
	}
	
	public static List<List<String>> getExchangeStock(String[] exchangeIds) throws Exception
	{
		List<List<String>> result = new ArrayList<List<String>>();
		List<List<String>> fullStock = getUniverseStock();
		for(int i=0; i<fullStock.size(); i++)
		{
			for (int j=0; j<exchangeIds.length; j++)
			{
				if (fullStock.get(i).get(0).trim().equals(exchangeIds[j])) result.add(fullStock.get(i));
			}
			
		}
		return result;
	}
	
	public static List<String> getExchangeShareClassId(String[] exchangeIds) throws Exception
	{
		List<String> result = new ArrayList<String>();
		List<List<String>> fullStock = getUniverseStock();
		for(int i=0; i<fullStock.size(); i++)
		{
			for (int j=0; j<exchangeIds.length; j++)
			{
				if (fullStock.get(i).get(1).trim().equals(exchangeIds[j])) result.add(fullStock.get(i).get(STOCK_SHARECLASSID));
			}
		}
		return result;
	}
	
	//It will change the Symbol to TenForce Symbol
	public static List<List<String>> getMSNUniverseStock() throws Exception
	{
		List<List<String>> result = getUniverseStock();
		for (int i = 0; i < result.size(); i++)
		{
			if (result.get(i).get(1).contains("."))		result.get(i).set(1, result.get(i).get(1).replace(".", "/"));
			if (result.get(i).get(1).equals("XOM") && result.get(i).get(0).equals("BUE"))		result.get(i).set(1, "AR-XOM");
			if (result.get(i).get(1).equals("HNP") && result.get(i).get(0).equals("BUE"))		result.get(i).set(1, "AR-HNP");
		}
		for (int i = 0; i < result.size(); i++)
		{
			System.out.println("ExchangeId: " + result.get(i).get(0) + "\tSymbol: " + result.get(i).get(1) + "\tShareClassId: " + result.get(i).get(2));
		}
		return result;
	}
	
	//Change the Symbol to TenForce Symbol
	public static String getTenForceSymbol(String exchange, String MSSymbol)
	{
		String result = MSSymbol;
		if (MSSymbol.contains("."))  result = MSSymbol.replace(".", "/");
		if ((MSSymbol.equals("XOM") || MSSymbol.equals("HNP")) && exchange.equals("BUE"))		result = "AR-" + MSSymbol;
		return result;
	}
	
}
