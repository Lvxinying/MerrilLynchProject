package com.morningstar.FundAutoTest;

import java.text.ParseException;


import org.slf4j.Logger;


public class Utils {

	public static void main(String[] args) throws ParseException {
	}

	public static String includeSquareBracket(String... value) {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		int i = 0;
		for (String str : value) {
			sb.append(str);
			i++;
			if (value.length != i)
				sb.append(",");
		}
		sb.append("]");
		return sb.toString();
	}

	public static String getActualExpectedMessage(String field, Object actual, Object expected) {
		String message = "{" + field + "} actual:" + Utils.includeSquareBracket(actual) + "|expected:" + Utils.includeSquareBracket(expected);
		return message;
	}
	

	public static String includeSquareBracket(Object value) {
		return "[" + value + "]";
	}

	public static void segmentMessage(Logger logger, String message) {
		logger.warn(message);
	}

	public static void segmentMessageForRoot(Logger logger, String message) {
		logger.warn("\n++++++++++++++" + message + "++++++++++++++");
	}

	public static String toString(Object o) {
		return o == null ? null : String.valueOf(o);
	}

	public static String checkNull(String str) {
		if ("".equals(str))
			return null;
		return str;
	}

	public static boolean isDigital(String key) {
		if (key != null)
			return key.matches("^[0-9]*$");
		else {
			return false;
		}
	}
	
	public static boolean isISIN(String key){
          if (key != null)
  			return key.matches("[A-Za-z]{2}[A-Za-z0-9]{10}$");
  		else {
  			return false;
  		}
	}

}
