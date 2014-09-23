package com.morningstar.FundAutoTest.source;

import java.io.InputStream;
import java.util.HashMap;

import com.morningstar.FundAutoTest.HttpConnection;
import com.morningstar.FundAutoTest.XmlHelper;
import com.morningstar.FundAutoTest.commons.ResourceManager;

public class GID {
	public static final String LINK = "http://globalid.morningstar.com/GIDDataIO/feed/asmx/Axis.asmx/GetYAxisDataTable?outputDPs=-2,-3,-5,1,21,45,53,70,84,91,100,103,120,254&inputQueryString=%s=";

	public static void main(String[] args) {
		// getDataMapFromGID("I3", "E0ARG003CM");
		// getDataMapFromGID("I5", "0P000000N3");
		getDataMapFromGID("I5", "0P000002RH");
	}

	public static HashMap<String, String> getDataMapFromGID(String queryType, String id) {
		HashMap<String, String> GIDMap = new HashMap<String, String>();
		initXmlHelper(queryType, id);

		String companyId = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/I2");
		String InvestmentId = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/I3");
		String shareClassId = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/I5");
		String companyName = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D1");
		String CIK = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D21");
		String InvestmentParentInvId = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D45");
		String InvestmentType = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D53");
		String CUSIP = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D70");
		String SEDOL = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D84");
		String symbol = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D91");
		String exchangeId = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D100").replace("EX$$$$X", "");
		String ISIN = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D120");
		String DRType = XmlHelper.getValueFromInputStream("/NewDataSet/GID[1]/D254");

		GIDMap.put("companyId", companyId);
		GIDMap.put("InvestmentId", InvestmentId);
		GIDMap.put("shareClassId", shareClassId);
		GIDMap.put("companyName", companyName);
		GIDMap.put("exchangeId", exchangeId);
		GIDMap.put("symbol", symbol);
		GIDMap.put("CUSIP", CUSIP);
		GIDMap.put("CIK", CIK);
		GIDMap.put("ISIN", ISIN);
		GIDMap.put("SEDOL", SEDOL);
		GIDMap.put("InvestmentType", InvestmentType);
		GIDMap.put("DRType", DRType);
		GIDMap.put("InvestmentParentInvId", InvestmentParentInvId);

		return GIDMap;
	}

	public static HashMap<String, String> getParentStock(String queryType, String id) {
		HashMap<String, String> GIDMap = new HashMap<String, String>();

		initXmlHelper(queryType, id);

		int num = XmlHelper.getNodeCount("/NewDataSet/GID");

		for (int i = 1; i <= num; i++) {
			String performanceStatus = XmlHelper.getValueFromInputStream("/NewDataSet/GID[" + i + "]/D103");

			if (performanceStatus!=null && !"0".equals(performanceStatus)) {

				String companyName = XmlHelper.getValueFromInputStream("/NewDataSet/GID[" + i + "]/D1");
				String symbol = XmlHelper.getValueFromInputStream("/NewDataSet/GID[" + i + "]/D91");
				String d100 = XmlHelper.getValueFromInputStream("/NewDataSet/GID[" + i + "]/D100");

				String exchangeId = null;

				if (d100 != null) {
					if (d100.startsWith("EX$$$$X"))
						exchangeId = d100.replace("EX$$$$X", "");
					else if (d100.startsWith("EX$$$$")) {
						exchangeId = d100.replace("EX$$$$", "");
					}
				}

				if (exchangeId != null && !"".equals(exchangeId))
					GIDMap.put(exchangeId, exchangeId + "-" + companyName + "-" + symbol);
			}
		}
		return GIDMap;
	}

	public static String getValue(String queryType, String id, String xpath) {
		initXmlHelper(queryType, id);

		String data = XmlHelper.getValueFromInputStream(xpath);
		return data;
	}

	private static void initXmlHelper(String queryType, String id) {
		InputStream in = HttpConnection.getGIDInputStream(ResourceManager.getGIDLoginUrl(), String.format(LINK, queryType) + id);
		XmlHelper.readStream(in);
	}

}
