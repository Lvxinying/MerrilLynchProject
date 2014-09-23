package com.morningstar.FundAutoTest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class DateUtil {
	public static void main(String[] args) throws ParseException {
		System.out.println(adjust2Months("yyyy-MM-dd", "2010-12-31", "2010-10-15"));
	}

	public static boolean adjust2Months(String pattern, String src, String dest) {
		SimpleDateFormat sd = new SimpleDateFormat(pattern);
		long a = 0L;
		try {
			a = sd.parse(src).getTime() - sd.parse(dest).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// 2 months long data is 5356800000L
		return Math.abs(a) <= 5356800000L;
	}

	public static boolean adjust2Months(Date src, Date dest) {
		long a = 0L;
		a = src.getTime() - dest.getTime();
		return Math.abs(a) <= 5356800000L;
	}

	public static String translateDate_yyyyMMdd(String str) {
		SimpleDateFormat sd = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sd1 = new SimpleDateFormat("yyyy-MM-dd");
		if (str != null)
			try {
				return sd1.format(sd.parse(str));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return null;
	}

	public static String translateDate(String str, String from, String to) {
		SimpleDateFormat sd = new SimpleDateFormat(from);
		SimpleDateFormat sd1 = new SimpleDateFormat(to);
		if (str != null)
			try {
				return sd1.format(sd.parse(str));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return null;
	}

	public static String getSpecifiedDayAfter(String pattern, String specifiedDay, int i) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat(pattern).parse(specifiedDay);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		int day = c.get(Calendar.DATE);
		c.set(Calendar.DATE, day + i);

		String dayAfter = new SimpleDateFormat(pattern).format(c.getTime());
		return dayAfter;
	}

	public static String getSpecifiedMonthAfter(String pattern, String specifiedMonth, int i) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat(pattern).parse(specifiedMonth);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		c.setTime(date);
		int month = c.get(Calendar.MONTH);
		c.set(Calendar.MONTH, month + i);

		String monthAfter = new SimpleDateFormat(pattern).format(c.getTime());
		return monthAfter;
	}

	public static String translateDate_EEEMMM(String str) {
		SimpleDateFormat sd = new SimpleDateFormat("EEE MMM d hh:mm:ss z yyyy", Locale.ENGLISH);
		SimpleDateFormat sd1 = new SimpleDateFormat("yyyy-MM-dd");
		if (str != null)
			try {
				return sd1.format(sd.parse(str));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return null;
	}

	public static String formatDate(String pattern, Date date) {
		SimpleDateFormat sd = new SimpleDateFormat(pattern);
		if (date != null)
			return sd.format(date);
		return null;
	}

	public static Date getYesterday() {
		Calendar day = Calendar.getInstance();
		day.add(Calendar.DATE, -1);
		return day.getTime();
	}

	public static String getYear(String str) {
		return str.split("-")[0];
	}

	public static String getYear(Date date, String pattern) {
		return formatDate(pattern, date).split("-")[0];
	}

	public static String getMonth(String str) {
		String month = str.split("-")[1];
		return month.startsWith("0") ? month.substring(1) : month;
	}

	public static String getMonth(Date date, String pattern) {
		String month = formatDate(pattern, date).split("-")[1];
		return month.startsWith("0") ? month.substring(1) : month;
	}

	public static String getCurrentDate(String timezone, String pattern) {

		return getNYear(timezone, 0, pattern);
	}

	public static String getCurrentYear(String timezone) {
		return getNYear(timezone, 0, "yyyy");
	}

	public static String getNYear(String timezone, int i, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);

		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.YEAR, i);
		TimeZone tz = TimeZone.getTimeZone(timezone);
		sdf.setTimeZone(tz);

		return sdf.format(calendar.getTime());
	}

	public static Date getNYearWithPattern(String timezone, int i, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);

		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.YEAR, i);
		TimeZone tz = TimeZone.getTimeZone(timezone);
		sdf.setTimeZone(tz);

		Date d = new Date();
		try {
			d = sdf.parse(sdf.format(calendar.getTime()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return d;
	}

	public static Date getNYear(String timezone, int i) {
		SimpleDateFormat sdf = new SimpleDateFormat();

		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.YEAR, i);
		TimeZone tz = TimeZone.getTimeZone(timezone);
		sdf.setTimeZone(tz);

		return calendar.getTime();
	}

	public static String getNMonth(String timezone, int i, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);

		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MONTH, i);
		TimeZone tz = TimeZone.getTimeZone(timezone);
		sdf.setTimeZone(tz);

		return sdf.format(calendar.getTime());
	}

	public static String getNDay(String timezone, int i, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);

		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, i);
		TimeZone tz = TimeZone.getTimeZone(timezone);
		sdf.setTimeZone(tz);

		return sdf.format(calendar.getTime());
	}

	public static String getFirstDateOfYear(String timeZone) {
		String year = getCurrentYear(timeZone);

		return "01/01/" + year;
	}

	public static String getFirstMonthOfYear(String timeZone) {
		String year = getCurrentYear(timeZone);

		return "01/" + year;
	}

	// Time is MM/dd/yyyy, Return Result is MM/yyyy
	// Time is YYYY-MM-dd, Return Result is MM/yyyy
	public static String getMonthYear(String time) {
		if (time.indexOf("/") > 0) {
			return time.substring(0, time.indexOf("/")) + time.substring(time.lastIndexOf("/"));
		} else
			return time.substring(time.indexOf("-") + 1, time.lastIndexOf("-")) + "/" + time.substring(0, time.indexOf("-"));
	}

	// 2011-10-05
	public static Calendar getNYear4Calendar(String timezone, int i, String pattern) {
		String date = getNYear(timezone, i, pattern);

		if ("MM/dd/yyyy".equals(pattern)) {
			String[] a = date.split("/");
			List<Integer> list = new ArrayList<Integer>();
			for (String s : a) {
				list.add(Integer.parseInt(s));
			}
			Calendar calendar = Calendar.getInstance();
			if (list.size() == 3)
				calendar.set(list.get(2), list.get(0) - 1, list.get(1), 0, 0, 0);
			return calendar;
		} else if ("yyyy-MM-dd".equals(pattern)) {
			String[] a = date.split("-");
			List<Integer> list = new ArrayList<Integer>();
			for (String s : a) {
				list.add(Integer.parseInt(s));
			}
			Calendar calendar = Calendar.getInstance();
			if (list.size() == 3)
				calendar.set(list.get(0), list.get(1) - 1, list.get(2), 0, 0, 0);
			return calendar;
		}
		return null;
	}

	public static boolean compareDate(Date date1, Date date2) {
		return (date1.getTime() - date2.getTime()) >= 0;
	}

	public static boolean compareDate(String startdate, String endDate, String pattern) {

		boolean a = false;
		try {
			SimpleDateFormat s = new SimpleDateFormat(pattern);
			a = s.parse(startdate).compareTo(s.parse(endDate)) > 0;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return a;
	}

	//
	public static int dayForWeek(String pTime) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
		Calendar c = Calendar.getInstance();
		c.setTime(sdf.parse(pTime));
		int dayForWeek = 0;
		if (c.get(Calendar.DAY_OF_WEEK) == 1) {
			dayForWeek = 7;
		} else {
			dayForWeek = c.get(Calendar.DAY_OF_WEEK) - 1;
		}
		return dayForWeek;
	}

	// Get This Date's previous Working Day if it is weekend, otherwise return
	// current day
	public static String getPreWorkingDay(String pTime, String pattern) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Calendar c = Calendar.getInstance();
		c.setTime(sdf.parse(pTime));

		if (c.get(Calendar.DAY_OF_WEEK) == 1)
			c.add(Calendar.DATE, -2);
		if (c.get(Calendar.DAY_OF_WEEK) == 7)
			c.add(Calendar.DATE, -1);
		return sdf.format(c.getTime());
	}

	// Get This Date's later Working Day if it is weekend, otherwise return
	// current day
	public static String getLateWorkingDay(String pTime, String pattern) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		Calendar c = Calendar.getInstance();
		c.setTime(sdf.parse(pTime));

		if (c.get(Calendar.DAY_OF_WEEK) == 1)
			c.add(Calendar.DATE, 1);
		if (c.get(Calendar.DAY_OF_WEEK) == 7)
			c.add(Calendar.DATE, 2);
		return sdf.format(c.getTime());
	}

	/**
	 * judge startDate whether endDate's latest work day.
	 * 
	 * @param startDate
	 *            (pattern:'yyyy-MM-dd','MM/dd/yyyy')
	 * @param endDate
	 *            (pattern:'yyyy-MM-dd','MM/dd/yyyy')
	 * @return
	 */
	public static boolean isOngoing(String startDate, String endDate) {
		Date sDate = null;
		Date eDate = null;
		String pattern = "yyyy-MM-dd";
		if (startDate.contains("-")) {
			sDate = parseTime(startDate, pattern);
		}
		if (startDate.contains("/")) {
			sDate = parseTime(formatDate(pattern, parseTime(startDate, "MM/dd/yyyy")), pattern);
		}
		if (endDate.contains("-")) {
			eDate = parseTime(endDate, "yyyy-MM-dd");
		}
		if (endDate.contains("/")) {
			eDate = parseTime(formatDate(pattern, parseTime(endDate, "MM/dd/yyyy")), pattern);
		}
		List<Date> latestDates = getLatestWorkday(eDate);
		if (sDate.after(eDate)) {
			return false;
		}
		for (Date date : latestDates) {
			Date thisDate = parseTime(formatDate("yyyy-MM-dd", date), "yyyy-MM-dd");
			if (thisDate.equals(sDate)) {
				return true;
			}
		}
		return false;
	}

	private static List<Date> getLatestWorkday(Date endDate) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(endDate);
		List<Date> dates = new ArrayList<Date>();
		if (cal.get(Calendar.DAY_OF_WEEK) == 1) {
			cal.add(Calendar.DAY_OF_MONTH, -2);
			dates.add(cal.getTime());
		}
		if (cal.get(Calendar.DAY_OF_WEEK) == 2) {
			cal.setTime(endDate);
			cal.add(Calendar.DAY_OF_MONTH, -3);
			dates.add(endDate);
			dates.add(cal.getTime());
		}
		if (cal.get(Calendar.DAY_OF_WEEK) == 7) {
			cal.setTime(endDate);
			cal.add(Calendar.DAY_OF_MONTH, -1);
			dates.add(cal.getTime());
		} else {
			cal.setTime(endDate);
			cal.add(Calendar.DAY_OF_MONTH, -1);
			dates.add(cal.getTime());
			dates.add(endDate);
		}
		return dates;
	}

	public static Date parseTime(String date, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		try {
			return sdf.parse(date);
		} catch (ParseException e) {
			return null;
		}
	}

	// get the latest day in this month, Return Result is MM/dd/yyyy
	public static String getLastDayInMonth(String time) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
		Calendar c = Calendar.getInstance();
		c.setTime(sdf.parse(time));
		String[] a = time.split("/");
		if (a.length == 3)
			return a[0] + "/" + Integer.toString(c.get(Calendar.DAY_OF_MONTH)) + "/" + a[2];
		if (a.length == 2)
			return a[0] + "/" + Integer.toString(c.get(Calendar.DAY_OF_MONTH)) + "/" + a[1];
		return null;
	}

	public static String getFirstDayInMonth(String time) {
		String[] a = time.split("/");
		if (a.length == 3)
			return a[0] + "/01/" + a[2];
		if (a.length == 2)
			return a[0] + "/01/" + a[1];
		return null;
	}

}
