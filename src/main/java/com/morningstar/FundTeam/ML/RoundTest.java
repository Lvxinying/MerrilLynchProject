package com.morningstar.FundTeam.ML;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;

import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.FundAutoTest.commons.Helper;

public class RoundTest {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws SQLException, Exception {		
		Double actYieldRateInDb = Double.parseDouble("3.514");
		Double realYieldRateInDb = Helper.setDoublePrecision(actYieldRateInDb, 3, BigDecimal.ROUND_HALF_DOWN);
		Double realYieldRateInFile = Double.parseDouble("3.515");
		
		System.out.println("realYieldRateInDb: "+realYieldRateInDb);
		System.out.println("realYieldRateInFile: "+realYieldRateInFile);
		if(!realYieldRateInDb.equals(realYieldRateInFile)){
			System.out.println(realYieldRateInFile - realYieldRateInDb);
			if(realYieldRateInFile - realYieldRateInDb != 9.999999999621423E-6){
				System.out.println("realYieldRateInDb: " + realYieldRateInDb);
				System.out.println("realYieldRateInFile: " + realYieldRateInFile);
			}			
		}
	}

}
