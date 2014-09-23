package com.morningstar.FundAutoTest.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import com.morningstar.FundAutoTest.XmlHelper;

public class Exchange {
	static String fileName = "config/ExchangeId.xml";
	static String Exchange = "/ExchangeList/Exchange";
	static String exchangeName = "/ExchangeList/Exchange[%d]/@ExchangeName";
	static String exchangeId = "/ExchangeList/Exchange[%d]/@Id";

	public static String getExchangeName(String id) {
		FileInputStream fis = null;

		try {
			fis = new FileInputStream(new File(fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		XmlHelper.readStream(fis);

		int num = XmlHelper.getNodeCount(Exchange);
		for (int i = 1; i <= num; i++) {
			if (id.equals(XmlHelper.getValueFromInputStream(String.format(exchangeId, i)))) {
				return XmlHelper.getValueFromInputStream(String.format(exchangeName, i));
			}
		}
		return "";

	}

	public static String getExchangeId(String name) {
		FileInputStream fis = null;

		try {
			fis = new FileInputStream(new File(fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		XmlHelper.readStream(fis);

		int num = XmlHelper.getNodeCount(Exchange);
		for (int i = 1; i <= num; i++) {
			if (name.equals(XmlHelper.getValueFromInputStream(String.format(exchangeName, i)))) {
				return XmlHelper.getValueFromInputStream(String.format(exchangeId, i));
			}
		}
		return "";

	}

}
