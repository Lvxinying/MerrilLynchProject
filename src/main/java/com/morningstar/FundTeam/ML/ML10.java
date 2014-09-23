package com.morningstar.FundTeam.ML;

import java.io.File;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.ExcelHelper;
import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.testbase.Base;
import java.sql.SQLException;
import java.text.ParseException;

public class ML10 extends Base{

	/**
	 * @author shou
	 * @throws Exception 
	 * @throws SQLException 
	 */
	
	@Test(description = "Testing whether end date has adjusted to calendar day!")
//第一个参数为	
	public static void testEndDate() throws Exception{		
		boolean testResult = true;
		HashMap<String,String> perfIdEndDateMap = new HashMap<String,String>();
		String sqlToGetPerfIdEndDate = "SELECT PerformanceId,EndDate FROM CurrentData.dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '3'";
		perfIdEndDateMap = DBCommons.getDataHashMap(sqlToGetPerfIdEndDate, Database.MsSQL1);
		Set perfIdKeySet = perfIdEndDateMap.keySet();
		if(!perfIdKeySet.isEmpty()){
			Iterator<String> it = perfIdKeySet.iterator();
			while(it.hasNext()){
				String perfIdKey = it.next();
//获取在DB中实际的日期
				String strActDate = perfIdEndDateMap.get(perfIdKey);
				Date actDate = Helper.dateParse(strActDate, "yyyy-MM-dd");
				Date lastDateOfMonth = Helper.lastDayOfMonth(actDate);
//不等于0说明时间不相等				
				if(actDate.compareTo(lastDateOfMonth) != 0){
					testResult = false;
					System.out.println("[FAILED]There have some records contain the date not the end calendar day!");
					System.out.println("Failed PerformanceId is: " + perfIdKey);
					System.out.println("String Date in DB is: " + strActDate);
					System.out.println("Bad Date in DB is: " + actDate);
					System.out.println("The last Date of Month is: " + lastDateOfMonth);
				}
			}
			if(testResult == true){
				System.out.println("[SUCCESS]All Bonds have the end of calendar date!");
			}
			Assert.assertTrue(testResult);
		}
	}	

	
	@Test(description = "Testing for all Bond that need to calculated!")
	public static void testRowDataComplete() throws Exception{
		boolean testResult = true;
//SQL		
		String sqlToGetRequiredCalculatedCount = "SELECT COUNT(PerformanceId) FROM CurrentData.dbo.MLBetaPerformanceAttributes WHERE MLUniverseType = '3'";
		String sqlToGetActuralCalculatedCount = "SELECT COUNT(PerformanceId) FROM CurrentData.dbo.ML3YearBetaForMonthEnd WHERE MLUniverseType = '3'";
		
		String strRequiredCount = DBCommons.getData(sqlToGetRequiredCalculatedCount, Database.MsSQL3);
		int requiredCount = Integer.parseInt(strRequiredCount);
		String strActuralCount = DBCommons.getData(sqlToGetActuralCalculatedCount, Database.MsSQL1);
		int acturalCount = Integer.parseInt(strActuralCount);
		
		if(requiredCount != acturalCount){
			System.out.println("[FAILED]Not all Bonds have been calculated!");
			System.out.println("RequiredCount is: " + requiredCount);
			System.out.println("ActuralCount is: " + acturalCount);
			testResult = false;
		}
		else{
			System.out.println("[SUCCESS]All Bonds have been calculated!");
			System.out.println("Calculated Bonds Count is: " + requiredCount);
		}
		Assert.assertTrue(testResult);
	}
	
	@Test(description = "Testing for Calculation veracity!")
	public static void testBondYTMCalculationveracity() throws Exception{
		
	}
	
	public static void main(String[] args) throws Exception {
		testRowDataComplete();
		testEndDate();
	}

}
