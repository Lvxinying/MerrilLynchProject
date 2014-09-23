package com.morningstar.FundAutoTest.source;

import java.util.List;

import com.morningstar.FundAutoTest.XmlHelper;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;

public class Tenfore {
	static final String TENFORE_URL = "http://platform-search/v2/securities/ids-mapping?q=OS06Y:%s&d=AC001,AC005";

	
	public static String getExchangeId(String shareClassId) {
		String url = String.format(TENFORE_URL, shareClassId);

		XmlHelper.readStream(url);
		int num = XmlHelper.getNodeCount("root/m/r/dv");
		for (int i = 1; i <= num; i++) {
			String xpath_d = "root/m/r/dv[" + i + "]/@d";
			String xpath_v = "root/m/r/dv[" + i + "]/@v";
			if ("AC005".equals(XmlHelper.getValueFromInputStream(xpath_d)))
				return XmlHelper.getValueFromInputStream(xpath_v);
		}
		return null;
	}
	
	

}
