package com.morningstar.FundAutoTest.source;

import java.io.InputStream;
import java.util.HashMap;

import org.apache.commons.codec.binary.Base64;

import com.morningstar.FundAutoTest.HttpConnection;
import com.morningstar.FundAutoTest.XmlHelper;
import com.morningstar.FundAutoTest.commons.ResourceManager;

public class PriceXOI {
	static final String RETURN_DETAIL = "/MarketPerformanceSnapshot/TrailingReturn/Return/ReturnDetail";

	public static HashMap<String, String> getDataMap(String link) {
		HashMap<String, String> map = new HashMap<String, String>();
		XmlHelper.readStream(getInputStream(link));
		int num = XmlHelper.getNodeCount(RETURN_DETAIL);
		for (int i = 1; i <= num; i++) {
			String returnDetailTimePeriod = RETURN_DETAIL + "[%d]" + "/@TimePeriod";
			String returnDetailValue = RETURN_DETAIL + "[%d]" + "/Value";
			returnDetailTimePeriod = String.format(returnDetailTimePeriod, i);
			returnDetailValue = String.format(returnDetailValue, i);
			String time_period = getData(link, returnDetailTimePeriod);
			if ("W1".equals(time_period)) {
				String value = getData(link, returnDetailValue);
				map.put("TrailingMarketReturn1W", value);
			} else if ("M1".equals(time_period)) {
				String value = getData(link, returnDetailValue);
				map.put("TrailingMarketReturn1M", value);
			} else if ("M12".equals(time_period)) {
				String value = getData(link, returnDetailValue);
				map.put("TrailingMarketReturn1Y", value);
			}
		}
		return map;

	}

	public static String getData(String link, String xPath) {
		XmlHelper.readStream(getInputStream(link));
		boolean hasException = XmlHelper.getNodeCount("//XOIException") == 1;
		if(hasException)
			return null;
		else
			return XmlHelper.getValueFromInputStream(xPath);
	}

	public static InputStream getInputStream(String link) {
		return HttpConnection.getPriceXOIInputStream(link, getToken());
	}

	private static String getToken() {
		String user = ResourceManager.getXoi_user();
		String pwd = ResourceManager.getXoi_password();
		String creds = user + ":" + pwd;
		return Base64.encodeBase64String(creds.getBytes());
	}

}