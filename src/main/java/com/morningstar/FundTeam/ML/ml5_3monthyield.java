package com.morningstar.FundTeam.ML;

import com.morningstar.FundAutoTest.commons.testbase.Base;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.Assert;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.morningstar.FundAutoTest.XmlHelperNew;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.testbase.Base;

import com.morningstar.FundAutoTest.commons.Helper;
import com.morningstar.FundAutoTest.commons.ExcelHelper;

import com.morningstar.data.tsapi.*;
import com.morningstar.data.tsapi.blobData.*;


public class ml5_3monthyield {
	@BeforeClass(description = "Preparing:Get PerformanceID from OIDEVOUTPUTDB81!")
	private static List<String> getPerforcemantID() throws SQLException{
		String sqlPath = "./config/Merrill_Lynch/ML-5/GetAverage3MonthYieldPerformanceID.sql";
		String SQL_get_PerformanceID = Helper.readFileByLines(sqlPath);
		List<String> pIDList = new ArrayList<String>();
		System.out.println("Starting to get PerformanceID! It may take a long time,please wait... ...");
		try{
        pIDList = DBCommons.getDataList(SQL_get_PerformanceID, Database.MsSQL1);
		}catch (Exception e) {
			e.getMessage();
		}
		System.out.println("NOTICE:[Getting PerformanceID process has been finished!]");
		return pIDList;
	}
	
	@BeforeClass(description = "Preparing:Get PerformanceID from OIDEVOUTPUTDB81!")
	private static List<String> get3MonthYieldRecords() throws SQLException{
		String sqlPath = "./config/Merrill_Lynch/ML-5/GetAverage3MonthYieldRecords.sql";
		String SQL_get_3MonthYieldRecords = Helper.readFileByLines(sqlPath);
		List<String> recordsList = new ArrayList<String>();
		System.out.println("Starting to get Average3MonthYield records! It may take a long time,please wait... ...");
		try{
			recordsList = DBCommons.getDataList(SQL_get_3MonthYieldRecords, Database.MsSQL1);			
		}catch (Exception e) {
			e.getMessage();
		}
		System.out.println("NOTICE:[Getting Average3MonthYield records process has been finished!]");
		return recordsList;
	}
	public static void main(String[] args) throws Exception{
//		getPerforcemantID();
		get3MonthYieldRecords();
	}
}
