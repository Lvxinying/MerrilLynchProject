package com.morningstar.FundAutoTest;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Random;

public class NumberUtil {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		double a = 199.919025499999;
		System.out.println(setScale(a, 6));
	}

	public static String mutiBy(BigDecimal b, Object mutiByNumber, int scale) {
		if (b == null)
			return null;

		if (mutiByNumber != null) {
			if (mutiByNumber instanceof String)
				return b.multiply(new BigDecimal((String) mutiByNumber)).setScale(scale, BigDecimal.ROUND_HALF_UP).toString();
			if (mutiByNumber instanceof Double)
				return b.multiply(new BigDecimal((Double) mutiByNumber)).setScale(scale, BigDecimal.ROUND_HALF_UP).toString();
			if (mutiByNumber instanceof BigDecimal)
				return b.multiply((BigDecimal) mutiByNumber).setScale(scale, BigDecimal.ROUND_HALF_UP).toString();
			if (mutiByNumber instanceof Integer)
				return b.multiply(new BigDecimal((Integer) mutiByNumber)).setScale(scale, BigDecimal.ROUND_HALF_UP).toString();
		}
		return null;
	}

	public static String setScale(Object object, int number) {
		if (object != null) {
			if (object instanceof String)
				return new BigDecimal((String) object).setScale(number, BigDecimal.ROUND_HALF_UP).toString();
			if (object instanceof Double)
				return new BigDecimal((Double) object).setScale(number, BigDecimal.ROUND_HALF_UP).toString();
			if (object instanceof BigDecimal)
				return ((BigDecimal) object).setScale(number, BigDecimal.ROUND_HALF_UP).toString();
		}
		return null;
	}

	public static String divideBy1(Object object, int scale) {
		if (object != null) {

			if (object instanceof String) {
				if (Double.parseDouble((String) object) + 1 == 1)
					return new BigDecimal(0).setScale(scale, BigDecimal.ROUND_HALF_UP).toString();
				else
					return new BigDecimal(1 / (Double.parseDouble((String) object))).setScale(scale, BigDecimal.ROUND_HALF_UP).toString();

			}
			if (object instanceof Double) {
				if ((Double) object + 1 == 1)
					return new BigDecimal(0).setScale(scale, BigDecimal.ROUND_HALF_UP).toString();
				else
					return new BigDecimal(1 / (Double) object).setScale(scale, BigDecimal.ROUND_HALF_UP).toString();
			}

			if (object instanceof BigDecimal) {
				return (new BigDecimal(1).divide((BigDecimal) object, scale, BigDecimal.ROUND_HALF_UP)).toString();
			}
		}
		return null;
	}

	public static String parse(Object obj) {
		if (obj != null) {
			if (obj instanceof Long)
				if ((Long) obj == Long.MIN_VALUE)
					return null;
			if (obj instanceof Integer)
				if ((Integer) obj == Integer.MIN_VALUE)
					return null;
			return String.valueOf(obj);
		} else {
			return null;
		}

	}

	public static int getRandom(int number) {
		Random random = new Random();
		return random.nextInt(number);
	}

	public static Object removeZero(Object obj) {
		if (obj != null) {
			DecimalFormat decimalFormat = new DecimalFormat("#,##0.######");
			int scale = 6;
			String value = Utils.toString(obj);

			if (value.contains(".000000") || value.indexOf(".") + scale > value.length() - 1) {

				if (obj instanceof String) {
					BigDecimal bigDecimal = new BigDecimal((String) obj);
					return decimalFormat.format(bigDecimal);
				}

				if (obj instanceof BigDecimal) {
					return decimalFormat.format((BigDecimal) obj);
				}
			}
			return obj;
		} else {
			return null;
		}
	}
}
