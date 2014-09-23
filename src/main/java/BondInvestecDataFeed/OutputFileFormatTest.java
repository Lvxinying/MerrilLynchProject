package BondInvestecDataFeed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.morningstar.FundAutoTest.commons.Helper;

public class OutputFileFormatTest {

	/**
	 * testing for output data file's format
	 * Refer to 'Investec Feed Format_8.13.2014.xlsx'
	 */
	Logger log = Logger.getLogger(OutputFileFormatTest.class);
	private static String OutPutFilePath = "C:/HJG_WORK/HJG_Project/Bond_Investec_DataFeed/Sample Output File 0814.txt";
	
	private List<String> loadMappingContent(String outPutFilePath) {
		long startTime = System.currentTimeMillis();
		List<String> list = new ArrayList<String>();
		try {
			list = Helper.readFileList(outPutFilePath);
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		long endTime = System.currentTimeMillis() - startTime;
		if (!list.isEmpty()) {
			log.info("Investec data feed output has been loaded,total cost: "
					+ endTime + " ms");
		}
		return list;
	}
	
//ISIN Format QA checking(Bad case size>12)
	private void testISINFormat(List<String> lines){
		String ISIN = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				ISIN = elements[0];
				if(ISIN.length() > 12){
					log.error("ISIN format: The ISIN in line number="+count+" contains more than 12 digits!Actural size is="+ISIN.length());
				}
			}
			log.info("ISIN format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}

//SEDOL Format QA checking(Bad case size>7)
	private void testSEDOLFormat(List<String> lines){
		String SEDOL = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				SEDOL = elements[1];
				if(SEDOL.length() > 7){
					log.error("SEDOL format: The SEDOL in line number="+count+" contains more than 7 digits!Actural size is="+SEDOL.length());
				}
			}
			log.info("SEDOL format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//Symbol Format QA checking(Bad case size>4)
	private void testSymbolFormat(List<String> lines){
		String Symbol = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				Symbol = elements[2];
				if(Symbol.length() > 4){
					log.error("Symbol format: The Symbol in line number="+count+" contains more than 4 digits!Actural size is="+Symbol.length());
				}
			}
			log.info("Symbol format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//Country Id Format QA checking(Bad case size>2)
	private void testCountryFormat(List<String> lines){
		String CountryId = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				CountryId = elements[3];
				if(CountryId.length() > 2){
					log.error("CountryId format: The CountryId in line number="+count+" contains more than 4 digits!Actural size is="+CountryId.length());
				}
			}
			log.info("CountryId format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//Description Format QA checking(Bad case size>120)
	private void testDescriptionFormat(List<String> lines){
		String Description = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				Description = elements[4];
				if(Description.length() > 120){
					log.error("Description format: The Description in line number="+count+" contains more than 120 digits!Actural size is="+Description.length());
				}
			}
			log.info("Description format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//InstrType Format QA checking(Bad case size>2  spec有问题)
	private void testInstrTypeFormat(List<String> lines){
		String InstrType = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				InstrType = elements[5];
				if(InstrType.length() > 2){
					log.error("InstrType format: The InstrType in line number="+count+" contains more than 2 digits!Actural size is="+InstrType.length());
				}
			}
			log.info("InstrType format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//IssuerName Format QA checking(Bad case size>120)	
	private void testIssuerNameFormat(List<String> lines){
		String IssuerName = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				IssuerName = elements[6];
				if(IssuerName.length() > 120){
					log.error("IssuerName format: The IssuerName in line number="+count+" contains more than 120 digits!Actural size is="+IssuerName.length());
				}
			}
			log.info("IssuerName format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//Coupon Format QA checking(Bad case size>9, decimal size>4)	
	private void testCouponFormat(List<String> lines){
		String Coupon = "";
		int decimalSize = 0;
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				Coupon = elements[7];
				if(Coupon.length() > 9){
					log.error("Coupon format: The Coupon in line number="+count+" contains more than 9 digits!Actural size is="+Coupon.length());
				}else{
					if(Helper.isDecimal(Coupon)){
						decimalSize = Helper.getDecimalScale(Coupon);
						if(decimalSize > 4){
							log.error("Coupon format: The decimal size of coupon in line number="+count+" contains more than 4 digits!Actural decimal size is="+decimalSize);
						}
					}
				}
			}
			log.info("Coupon format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}

//Frequency Format QA checking(bad case size>1)
	private void testFrequencyFormat(List<String> lines){
		String Frequency = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				Frequency = elements[8];
				if(Frequency.length() > 1){
					log.error("Frequency format: The Frequency in line number="+count+" contains more than 1 digits!Actural size is="+Frequency.length());
				}
			}
			log.info("Frequency format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//Frequency Unit Format QA checking(bad case size>2)
	private void testFrequencyUnitFormat(List<String> lines){
		String FrequencyUnit = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				FrequencyUnit = elements[9];
				if(FrequencyUnit.length() > 2){
					log.error("FrequencyUnit format: The FrequencyUnit in line number="+count+" contains more than 2 digits!Actural size is="+FrequencyUnit.length());
				}
			}
			log.info("FrequencyUnit format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//InceptionDateId Format QA checking(bad case:not a date or can't match YYYY-MM-DD format)	
	private void testInceptionDateIdFormat(List<String> lines){
		String InceptionDateId = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				InceptionDateId = elements[10];
				if(InceptionDateId.isEmpty()){
					continue;
				}
				else if(!InceptionDateId.contains("-")){
					log.error("InceptionDateId format: The InceptionDateId in line number="+count+" is not a Date Format like!Actural content is="+InceptionDateId);
				}else{
					String[] date = InceptionDateId.split("-", 3);
					if(date.length != 3){
						log.error("InceptionDateId format: The InceptionDateId in line number="+count+" contains an invalid date format!Actural content is="+InceptionDateId);
					}
				}
			}
			log.info("InceptionDateId format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}

//MaturityDateId Format QA checking(bad case:not a date or can't match YYYY-MM-DD format)
	private void testMaturityDateIdFormat(List<String> lines){
		String MaturityDateId = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				MaturityDateId = elements[11];
				if(MaturityDateId.isEmpty()){
					continue;
				}
				else if(!MaturityDateId.contains("-")){
					log.error("MaturityDateId format: The MaturityDateId in line number="+count+" is not a Date Format like!Actural content is="+MaturityDateId);
				}else{
					String[] date = MaturityDateId.split("-", 3);
					if(date.length != 3){
						log.error("MaturityDateId format: The MaturityDateId in line number="+count+" contains an invalid date format!Actural content is="+MaturityDateId);
					}
				}
			}
			log.info("MaturityDateId format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//IssueSize Format QA checking(bad case: not a numberic/it's defined as a bigint)
	private void testIssueSizeFormat(List<String> lines){
		String IssueSize = "";
//Verify whether the string is a number? 		
		String regEx = "^-?[0-9]+$";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				IssueSize = elements[12];
				if(IssueSize.isEmpty()){
					continue;
				}else{
					if(!IssueSize.matches(regEx)){
						log.error("IssueSize format: The IssueSize in line number="+count+" is not a number!Actural content is :"+IssueSize);
					}
				}				
			}
			log.info("IssueSize format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//TradingCurrency Format QA checking(bad case size>3)
	private void testTradingCurrencyFormat(List<String> lines){
		String TradingCurrency = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				TradingCurrency = elements[13];
				if(TradingCurrency.length() > 3){
					log.error("TradingCurrency format: The TradingCurrency in line number="+count+" contains more than 3 digits!Actural size is="+TradingCurrency.length());
				}
			}
			log.info("TradingCurrency format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//MinDenomination Format QA checking(bad case not a numberic/it's defined as a bigint)
	private void testMinDenominationFormat(List<String> lines){
		String MinDenomination = "";
//Verify whether the string is a number? 		
		String regEx = "^-?[0-9]+$";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				MinDenomination = elements[14];
				if(MinDenomination.isEmpty()){
					continue;
				}else{
					if(!MinDenomination.matches(regEx)){
						log.error("MinDenomination format: The MinDenomination in line number="+count+" is not a number!Actural content is :"+MinDenomination);
					}
				}				
			}
			log.info("MinDenomination format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//IncrementalInvestment Format QA checking(bad case not a numberic/it's defined as an int)	
	private void testIncrementalInvestmentFormat(List<String> lines){
		String IncrementalInvestment = "";
//Verify whether the string is a number? 		
		String regEx = "^-?[0-9]+$";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				IncrementalInvestment = elements[15];
				if(IncrementalInvestment.isEmpty()){
					continue;
				}else{
					if(!IncrementalInvestment.matches(regEx)){
						log.error("IncrementalInvestment format: The IncrementalInvestment in line number="+count+" is not a number!Actural content is :"+IncrementalInvestment);
					}
				}				
			}
			log.info("IncrementalInvestment format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//CouponType Format QA checking(bad case size>1)
	private void testCouponTypeFormat(List<String> lines){
		String CouponType = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				CouponType = elements[16];
				if(CouponType.length() > 1){
					log.error("CouponType format: The CouponType in line number="+count+" contains more than 1 digits!Actural size is="+CouponType.length());
				}
			}
			log.info("CouponType format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//ModifiedDuration Format QA checking(Bad case size>9, decimal size>4)
	private void testModifiedDurationFormat(List<String> lines){
		String ModifiedDuration = "";
		int decimalSize = 0;
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				ModifiedDuration = elements[17];
				if(ModifiedDuration.length() > 9){
					log.error("ModifiedDuration format: The ModifiedDuration in line number="+count+" contains more than 9 digits!Actural size is="+ModifiedDuration.length());
				}else{
					if(Helper.isDecimal(ModifiedDuration)){
						decimalSize = Helper.getDecimalScale(ModifiedDuration);
						if(decimalSize > 4){
							log.error("ModifiedDuration format: The decimal size of ModifiedDuration in line number="+count+" contains more than 4 digits!Actural decimal size is="+decimalSize);
						}
					}
				}
			}
			log.info("ModifiedDuration format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}

//ModifiedDurationDate Format QA checking(bad case:not a date or can't match YYYY-MM-DD format)
	private void testModifiedDurationDateFormat(List<String> lines){
		String ModifiedDurationDate = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				ModifiedDurationDate = elements[18];
				if(ModifiedDurationDate.isEmpty()){
					continue;
				}
				else if(!ModifiedDurationDate.contains("-")){
					log.error("ModifiedDurationDate format: The ModifiedDurationDate in line number="+count+" is not a Date Format like!Actural content is="+ModifiedDurationDate);
				}else{
					String[] date = ModifiedDurationDate.split("-", 3);
					if(date.length != 3){
						log.error("ModifiedDurationDate format: The ModifiedDurationDate in line number="+count+" contains an invalid date format!Actural content is="+ModifiedDurationDate);
					}
				}
			}
			log.info("ModifiedDurationDate format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//ClosePrice Format QA checking(Bad case size>9, decimal size>4)
	private void testClosePriceFormat(List<String> lines){
		String ClosePrice = "";
		int decimalSize = 0;
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				ClosePrice = elements[19];
				if(ClosePrice.length() > 9){
					log.error("ClosePrice format: The ClosePrice in line number="+count+" contains more than 9 digits!Actural size is="+ClosePrice.length());
				}else{
					if(Helper.isDecimal(ClosePrice)){
						decimalSize = Helper.getDecimalScale(ClosePrice);
						if(decimalSize > 4){
							log.error("ClosePrice format: The decimal size of ClosePrice in line number="+count+" contains more than 4 digits!Actural decimal size is="+decimalSize);
						}
					}
				}
			}
			log.info("ClosePrice format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//TradeDate Format QA checking(bad case:not a date or can't match YYYY-MM-DD format)	
	private void testTradeDateFormat(List<String> lines){
		String TradeDate = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				TradeDate = elements[20];
				if(TradeDate.isEmpty()){
					continue;
				}
				else if(!TradeDate.contains("-")){
					log.error("TradeDate format: The TradeDate in line number="+count+" is not a Date Format like!Actural content is="+TradeDate);
				}else{
					String[] date = TradeDate.split("-", 3);
					if(date.length != 3){
						log.error("TradeDate format: The TradeDate in line number="+count+" contains an invalid date format!Actural content is="+TradeDate);
					}
				}
			}
			log.info("TradeDate format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//DirtyPrice Format QA checking(Bad case size>9, decimal size>4)
	private void testDirtyPriceFormat(List<String> lines){
		String DirtyPrice = "";
		int decimalSize = 0;
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				DirtyPrice = elements[21];
				if(DirtyPrice.length() > 9){
					log.error("DirtyPrice format: The DirtyPrice in line number="+count+" contains more than 9 digits!Actural size is="+DirtyPrice.length());
				}else{
					if(Helper.isDecimal(DirtyPrice)){
						decimalSize = Helper.getDecimalScale(DirtyPrice);
						if(decimalSize > 4){
							log.error("DirtyPrice format: The decimal size of DirtyPrice in line number="+count+" contains more than 4 digits!Actural decimal size is="+decimalSize);
						}
					}
				}
			}
			log.info("DirtyPrice format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}

//DirtyValuationDate Format QA checking(bad case:not a date or can't match YYYY-MM-DD format)
	private void testDirtyValuationDateFormat(List<String> lines){
		String DirtyValuationDate = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				DirtyValuationDate = elements[22];
				if(DirtyValuationDate.isEmpty()){
					continue;
				}
				else if(!DirtyValuationDate.contains("-")){
					log.error("DirtyValuationDate format: The DirtyValuationDate in line number="+count+" is not a Date Format like!Actural content is="+DirtyValuationDate);
				}else{
					String[] date = DirtyValuationDate.split("-", 3);
					if(date.length != 3){
						log.error("DirtyValuationDate format: The DirtyValuationDate in line number="+count+" contains an invalid date format!Actural content is="+DirtyValuationDate);
					}
				}
			}
			log.info("DirtyValuationDate format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//DaysAI Format QA checking(bad case not a numberic/it's defined as an int)
	private void testDaysAIFormat(List<String> lines){
		String DaysAI = "";
//Verify whether the string is a number? 		
		String regEx = "^-?[0-9]+$";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				DaysAI = elements[23];
				if(DaysAI.isEmpty()){
					continue;
				}else{
					if(!DaysAI.matches(regEx)){
						log.error("DaysAI format: The DaysAI in line number="+count+" is not a number!Actural content is :"+DaysAI);
					}
				}				
			}
			log.info("DaysAI format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//Next Coupon Payment Date Format QA checking(bad case:not a date or can't match YYYY-MM-DD format)
	private void testNextCouponPaymentDateFormat(List<String> lines){
		String NextCouponPaymentDate = "";
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				NextCouponPaymentDate = elements[24];
				if(NextCouponPaymentDate.isEmpty()){
					continue;
				}
				else if(!NextCouponPaymentDate.contains("-")){
					log.error("NextCouponPaymentDate format: The NextCouponPaymentDate in line number="+count+" is not a Date Format like!Actural content is="+NextCouponPaymentDate);
				}else{
					String[] date = NextCouponPaymentDate.split("-", 3);
					if(date.length != 3){
						log.error("NextCouponPaymentDate format: The NextCouponPaymentDate in line number="+count+" contains an invalid date format!Actural content is="+NextCouponPaymentDate);
					}
				}
			}
			log.info("NextCouponPaymentDate format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}

//YTM Format QA checking（Bad case size>9, decimal size>4）
	private void testYTMFormat(List<String> lines){
		String YTM = "";
		int decimalSize = 0;
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				YTM = elements[25];
				if(YTM.length() > 9){
					log.error("YTM format: The YTM in line number="+count+" contains more than 9 digits!Actural size is="+YTM.length());
				}else{
					if(Helper.isDecimal(YTM)){
						decimalSize = Helper.getDecimalScale(YTM);
						if(decimalSize > 4){
							log.error("YTM format: The decimal size of YTM in line number="+count+" contains more than 4 digits!Actural decimal size is="+decimalSize);
						}
					}
				}
			}
			log.info("YTM format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
//Current Yield（Bad case size>9, decimal size>4）	
	private void testCurrentYieldFormat(List<String> lines){
		String CurrentYield = "";
		int decimalSize = 0;
		int count = 0;
		if(!lines.isEmpty()){
			for(String lineStr:lines){
				count++;
				String[] elements = lineStr.split("\t", 27);
				CurrentYield = elements[26];
				if(CurrentYield.length() > 9){
					log.error("CurrentYield format: The CurrentYield in line number="+count+" contains more than 9 digits!Actural size is="+CurrentYield.length());
				}else{
					if(Helper.isDecimal(CurrentYield)){
						decimalSize = Helper.getDecimalScale(CurrentYield);
						if(decimalSize > 4){
							log.error("CurrentYield format: The decimal size of CurrentYield in line number="+count+" contains more than 4 digits!Actural decimal size is="+decimalSize);
						}
					}
				}
			}
			log.info("CurrentYield format test has finished!");
		}else{
			log.warn("Getting no lines contents in Investec output data feed file!");
		}
	}
	
	
	public static void main(String[] args) {
		OutputFileFormatTest test = new OutputFileFormatTest();
		List<String> lines = test.loadMappingContent(OutPutFilePath);
		test.testISINFormat(lines);
		test.testSEDOLFormat(lines);
//目前的sample output file有Symbol不符合format格式要求的数据		
//		test.testSymbolFormat(lines);
		test.testCountryFormat(lines);
		test.testDescriptionFormat(lines);
		test.testInstrTypeFormat(lines);
		test.testIssuerNameFormat(lines);
//目前的Sample output file的Counpon中有不符合小数点大小要求的数据		
//		test.testCouponFormat(lines);
		test.testFrequencyFormat(lines);
		test.testFrequencyUnitFormat(lines);
		test.testInceptionDateIdFormat(lines);
		test.testMaturityDateIdFormat(lines);
		test.testIssueSizeFormat(lines);
		test.testTradingCurrencyFormat(lines);
//目前有一行数据中的MinDenomination不符合格式要求，是个小数		
		test.testMinDenominationFormat(lines);
//目前有一行数据中的IncrementalInvestment不符合格式要求，是个小数	
		test.testIncrementalInvestmentFormat(lines);
		test.testCouponTypeFormat(lines);
//目前的Sample output file的ModifiedDurationFormat中有不符合小数点大小要求的数据			
//		test.testModifiedDurationFormat(lines);
		test.testModifiedDurationDateFormat(lines);
//目前的Sample output file的ClosePrice中有不符合小数点大小要求的数据		
//		test.testClosePriceFormat(lines);
		test.testTradeDateFormat(lines);
//目前的Sample output file的DirtyPrice中有不符合小数点大小要求的数据		
//		test.testDirtyPriceFormat(lines);
		test.testDirtyValuationDateFormat(lines);
//Sample output file中还没有存在这列的数据		
//		test.testDaysAIFormat(lines);
//Sample output file中还没有存在这列的数据		
//		test.testNextCouponPaymentDateFormat(lines);
//Sample output file中还没有存在这列的数据
//		test.testYTMFormat(lines);
//Sample output file中还没有存在这列的数据
//		test.testCurrentYieldFormat(lines);
		
	}

}
