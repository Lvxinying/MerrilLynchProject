package MarketPriceBackFeedToTSDB;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.morningstar.FundAutoTest.commons.CustomizedLog;
import com.morningstar.FundAutoTest.commons.DBCommons;
import com.morningstar.FundAutoTest.commons.Database;
import com.morningstar.data.tsapi.TSException;

public class EquityPriceCorpActionBaseObject {
	private static String logPath = "./log/TestLog/MarketPriceBackFeed/";
	private static String logName1 = "MissingRecord_DBI_EquityPriceCorpAction(BaseObject).log";
	private static String logName2 = "ContentChecking_DBI_EquityPriceCorpAction(BaseObject).log";
	static SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
//Get PerformanceIdList(EOD Mode)
	private static List<String> getUpdatedPerformanceIdListEODMode(String tableName,String createdOnTime) throws SQLException{
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//保留CREATEDON的使用
		if(createdOnTime.isEmpty()){
			createdOnTime = "2000-01-01";
		}
		String sqlToGetUpdatedPerformanceId = "SELECT DISTINCT PERFORMANCEID FROM ADT_EXCHANGEUNIVERSE EU"+
								 " JOIN ADT_NOTIFICATIONDRIVER ND ON ND.ID = EU.EXCHANGEID AND ND.STARTDATE = EU.EFFECTIVEDATE AND ND.TABLENAME = EU.TABLENAME"+
								 " WHERE ND.IDTYPE = 2 AND ND.TABLENAME = '" + tableName + "' AND ND.CREATEDON >= '" + createdOnTime + "'";
		list = DBCommons.getDataList(sqlToGetUpdatedPerformanceId, Database.Netezza1);
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got "+ list.size()+" performanceids(EOD MODE),time cost: "+endTime+" ms");
		return list;
	}

//Get PerformanceIdList(Individual Mode)
	private static List<String> getUpdatedPerformanceIdListIndividualMode(String testedTableName,int TsType, int testedIdCounts){
		List<String> list = new ArrayList<String>();
		long startTime = System.currentTimeMillis();
//选取IdType != 2的所有情况，且PROCESSNAME为DMRI for Equity或者DMRI for ETF才能保证是BASE OBJECT的		
		String sqlToGetUpdatedPerformanceIdList = "SELECT ID FROM ADT_NOTIFICATIONDRIVER WHERE PROCESSNAME IN('DMRI for Equity','DMRI for ETF') " +
				"AND IDTYPE != 2 TSTYPE = " + TsType + " AND TABLENAME = '" + testedTableName + "' LIMIT "+testedIdCounts;
		try {
			list = DBCommons.getDataList(sqlToGetUpdatedPerformanceIdList, Database.Netezza1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis() - startTime;
		System.out.println("[INFO]Got " + list.size() +" performanceids(Individual MODE), TSTYPE = "+ TsType + ",time cost: "+endTime+" ms");
		return list;
	}
	
//	检测Netezza到TsDB更新后的数据量是否保持一致
	public static void compareUpdatedData(TsBlobDataTypeBaseObject tsType, List<String> perfIdList,int bufferSize) throws ParseException{
 		Map<String,Map<String,List<String>>> netezzaValueMap = new HashMap<String,Map<String,List<String>>>();
 		Map<String,Map<String,List<String>>> tsdbValueMap = new HashMap<String,Map<String,List<String>>>();		
		int i = 0;
		int size = perfIdList.size();
		String strTsDB = "";
		Double doubleTsDB = null;
		Double doubleNetezza = null;
		List<ArrayList<String>> perfIdContainer = new ArrayList<ArrayList<String>>();

//Table DBI_EQUITYPRICECORPACTION				
		switch(tsType){									
		case tsShareSplitRatio:
//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsTenforeCurrencyTradingPrice =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsTenforeCurrencyTradingPrice++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsTenforeCurrencyTradingPrice*bufferSize,size - countTsTenforeCurrencyTradingPrice*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);		
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsSpinoff:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsSpinoff =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsSpinoff++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsSpinoff*bufferSize,size - countTsSpinoff*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											if(!strTsDB.equals(strNetezza)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsStockDistribution:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsStockDistribution =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsStockDistribution++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsStockDistribution*bufferSize,size - countTsStockDistribution*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsSpecialCashDividend:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsSpecialCashDividend =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsSpecialCashDividend++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsSpecialCashDividend*bufferSize,size - countTsSpecialCashDividend*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsDividendFrequency:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsDividendFrequency =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsDividendFrequency++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsDividendFrequency*bufferSize,size - countTsDividendFrequency*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsCashDividend:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsCashDividend =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsCashDividend++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsCashDividend*bufferSize,size - countTsCashDividend*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsCashDividendCurrencyHistory:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsCashDividendCurrencyHistory =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsCashDividendCurrencyHistory++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsCashDividendCurrencyHistory*bufferSize,size - countTsCashDividendCurrencyHistory*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				String currencyPrefix = "CU$$$$$";
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										strNetezza = currencyPrefix+strNetezza;
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											if(!strNetezza.equals(strTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsSpecialCashDividendDates:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsSpecialCashDividendDates =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsSpecialCashDividendDates++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsSpecialCashDividendDates*bufferSize,size - countTsSpecialCashDividendDates*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										Date dateTsDb = df2.parse(strTsDB);
										Date dateNetezza = df1.parse(strNetezza);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											if(!dateNetezza.equals(dateTsDb)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
//有问题			
		case tsSpecialCashDividendCurrencyHistory:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsSpecialCashDividendCurrencyHistory =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsSpecialCashDividendCurrencyHistory++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsSpecialCashDividendCurrencyHistory*bufferSize,size - countTsSpecialCashDividendCurrencyHistory*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				String currencyPrefix = "CU$$$$$";
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										strNetezza = currencyPrefix+strNetezza;
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											if(!strNetezza.equals(strTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsSplitDates:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsSplitDates =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsSplitDates++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsSplitDates*bufferSize,size - countTsSplitDates*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										Date dateTsDb = df2.parse(strTsDB);
										Date dateNetezza = df1.parse(strNetezza);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											if(!dateNetezza.equals(dateTsDb)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsStockDistributionDates:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsStockDistributionDates =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsStockDistributionDates++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsStockDistributionDates*bufferSize,size - countTsStockDistributionDates*bufferSize);
				long startTime1 = System.currentTimeMillis();			
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										Date dateTsDb = df2.parse(strTsDB);
										Date dateNetezza = df1.parse(strNetezza);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											if(!dateNetezza.equals(dateTsDb)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsRightsOfferingDates:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsRightsOfferingDates =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsRightsOfferingDates++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsRightsOfferingDates*bufferSize,size - countTsRightsOfferingDates*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);			
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										Date dateTsDb = df2.parse(strTsDB);
										Date dateNetezza = df1.parse(strNetezza);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											if(!dateNetezza.equals(dateTsDb)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsRightsOffering:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsRightsOffering =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsRightsOffering++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsRightsOffering*bufferSize,size - countTsRightsOffering*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(i);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
										i++;
									}
									i=0;
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsRightsOfferingAdjustmentFactor:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsRightsOfferingAdjustmentFactor =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				
				countTsRightsOfferingAdjustmentFactor++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsRightsOfferingAdjustmentFactor*bufferSize,size - countTsRightsOfferingAdjustmentFactor*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){
											doubleTsDB =  Double.valueOf(strTsDB);
											doubleNetezza = Double.valueOf(strNetezza);
											if(!doubleNetezza.equals(doubleTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			
		case tsRightsOfferingCurrencyHistory:
			//CreateLogFile
			try {
				CustomizedLog.creatCusomizedLogFile(logPath, logName1, "MissingRecord_EquityPriceCorpAction(BaseObject)");
				CustomizedLog.creatCusomizedLogFile(logPath, logName2, "ContentChecking_EquityPriceCorpAction(BaseObject)");
			} catch (IOException e1) {
				System.out.println("[ERROR]Creating Log file error,info: " +e1.getMessage());
			}
			System.out.println("[INFO]Begin to test values of EquityPriceCorpAction between Netezza and TsDB");
			System.out.println("[INFO]TsDB Mapping type:BaseObject--> TsDB env : Dev");
			System.out.println("[INFO]Netezza env:Dev env--> DB Name:MSNZDEV");
			System.out.println("[INFO]Total PerformanceId count is :  "+perfIdList.size());
			
			perfIdContainer = getSubList(perfIdList, bufferSize);
			int countTsRightsOfferingCurrencyHistory =0;
			for(List<String> sublist:perfIdContainer){
				Map<String,List<String>> dateValueListMapTsdb = new HashMap<String,List<String>>();
				Map<String,List<String>> dateValueListMapNetezza = new HashMap<String,List<String>>();
				List<String> valueListTsdb = new ArrayList<String>();
				List<String> valueListNetezza = new ArrayList<String>();				
				String currencyPrefix = "CU$$$$$";
				countTsRightsOfferingCurrencyHistory++;
//batch IDs testing once
				System.out.printf("[INFO]Start to load %d PerformanceId... ... \n",bufferSize);
				System.out.printf("[INFO]Prepare to test %d PerformanceIds, remaining %d PerformanceIds \n", countTsRightsOfferingCurrencyHistory*bufferSize,size - countTsRightsOfferingCurrencyHistory*bufferSize);
				long startTime1 = System.currentTimeMillis();				
				tsdbValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromTsDB(tsType, tsType.TsType, sublist);				
				long endTime1 = System.currentTimeMillis() - startTime1;
				System.out.println("[INFO]Loading data from TSDB has finished!Total cost: " + endTime1/(1000*60) + " min");
				
				long startTime2 = System.currentTimeMillis();
				try {
					netezzaValueMap = EquityPriceCorpActionBaseObjectUtil.getFullValueFromNetezza(tsType, Database.Netezza1, sublist, "DBI_EQUITYPRICECORPACTION");
				} catch (SQLException e) {
					System.out.println("[ERROR]Get " + tsType + " values from Netezza error!Info: "+e.getMessage());
				}
				long endTime2 = System.currentTimeMillis() - startTime2;
				System.out.println("[INFO]Loading data from Netezza has finished!Total cost: " + endTime2/(1000*60) + " min");
//Get missing date in TsDB
				long missDateStart = System.currentTimeMillis();
				for(String keyPerfId: netezzaValueMap.keySet()){
					List<String> tsdbDateList = new ArrayList<String>();
					List<String> netezzaDateList = new ArrayList<String>();
					List<String> missDateList = new ArrayList<String>();
					
					dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
					dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
//Get tsdb date list
					if(dateValueListMapTsdb != null && !dateValueListMapTsdb.isEmpty()){
						for(String dateStr:dateValueListMapTsdb.keySet()){
							tsdbDateList.add(dateStr);
						}
					}
//Get netezza date list
					if(dateValueListMapNetezza != null && !dateValueListMapNetezza.isEmpty()){
						for(String dateStr:dateValueListMapNetezza.keySet()){
							netezzaDateList.add(dateStr);
						}
					}
					missDateList = getMissingDate(tsdbDateList,netezzaDateList);
					if(!missDateList.isEmpty()){
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "[ERROR]Missing date in TsDB,maybe datas are missing belong to this date when load data to TsDB from Netezza!");
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "TsType NAME is: " + tsType);
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Performance ID is: "+keyPerfId);
						for(String strDate:missDateList){
							CustomizedLog.writeCustomizedLogFile(logPath+logName1, "Missing date in TsDB is: "+strDate);
						}			
						CustomizedLog.writeCustomizedLogFile(logPath+logName1, "================================================");
					}
				}
				long missDateEnd = System.currentTimeMillis() - missDateStart;
				System.out.println("[INFO]All missing date records have logged! Total cost: "+missDateEnd/1000+" s");
				
//Value compare
				long startValueCompareTime = System.currentTimeMillis();
				System.out.println("[INFO]Value comparing... ...");
				for(String keyPerfId: netezzaValueMap.keySet()){
					if(keyPerfId != null){
						dateValueListMapTsdb = tsdbValueMap.get(keyPerfId);
						dateValueListMapNetezza = netezzaValueMap.get(keyPerfId);
						for(String keyDateOre : dateValueListMapNetezza.keySet()){
//Netezza和TsDB中的日期格式不同，需要转换一下							
							Date keyDateOreFormat = df1.parse(keyDateOre);
							String keyDate = df2.format(keyDateOreFormat);
							if(keyDate!=null && dateValueListMapTsdb.containsKey(keyDate)){
								valueListTsdb =  dateValueListMapTsdb.get(keyDate);
								if(valueListTsdb.isEmpty() || valueListTsdb == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in TsDB!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}
								valueListNetezza = dateValueListMapNetezza.get(keyDateOre);
								if(valueListNetezza.isEmpty() || valueListNetezza == null){
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]No data found belongs to this date in Netezza!");
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
									CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
								}else{
									for(String strNetezza:valueListNetezza){
										strTsDB = valueListTsdb.get(0);
										strNetezza = currencyPrefix+strNetezza;
										if(!strNetezza.equals("NullValue") && !strTsDB.equals("NaN")){											
											if(!strNetezza.equals(strTsDB)){
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
												CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}
										}else if(strNetezza.equals("NullValue")){
												if(!strTsDB.equals("NaN")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
											}else if(strTsDB.equals("NaN")){
												if(!strNetezza.equals("NullValue")){
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "[ERROR]Value not match between Netezza and TsDB!");
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "TsType NAME is: " + tsType);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Performance ID is: "+keyPerfId);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch date is: "+keyDate);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in Netezza is: "+strNetezza);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "Mismatch value in TsDB is: "+strTsDB);
													CustomizedLog.writeCustomizedLogFile(logPath+logName2, "================================================");
												}
											}
										}
									}
								}				
							}
						}
					}
				}
				long endValueCompareTime = System.currentTimeMillis()-startValueCompareTime;
				System.out.println("[INFO]QA comparing finished!Total cost: "+endValueCompareTime/1000+" s");
			}
			break;
			default:
			break;
		}		
	}
	
	@SuppressWarnings("unchecked")
	private static List<ArrayList<String>> getSubList(List<String> perfIdListFull,int bufferSize){
		List<ArrayList<String>> listContainer = new ArrayList<ArrayList<String>>();
		ArrayList<String> list = new ArrayList<String>();
		int flag = 0;
		int buffer = bufferSize;
		int count = 0;
		int size = perfIdListFull.size();
		for(String perfId:perfIdListFull){
			flag++;
			if(flag%buffer !=0){
				list.add(perfId);
			}else{
				list.add(perfId);
				ArrayList<String> sublist = new ArrayList<String>();
				sublist = (ArrayList<String>) list.clone();
				listContainer.add(sublist);
				list.clear();
				flag=0;
				count++;
			}
			if(size-count*buffer>0 && size-count*buffer<buffer){
				for(int j=count*buffer;j<size;j++){
					list.add(perfIdListFull.get(j));
				}
				listContainer.add(list);
				break;
			}
		}
		return listContainer;
	}
	
	private static List<String> getMissingDate(List<String> tsdbDateList,List<String> netezzaDateList) throws ParseException{
		List<String> resultList = new ArrayList<String>();
		List<String> tempList = new ArrayList<String>();
		Date newDate;
		boolean bl = false;
		for(String dateStr:tsdbDateList){
			newDate = df2.parse(dateStr);
			dateStr = df1.format(newDate);
			tempList.add(dateStr);
		}
		bl = netezzaDateList.removeAll(tempList);
		if(bl){
			resultList = netezzaDateList;
		}
		if(!tempList.isEmpty()){
			tempList.clear();
		}
		return resultList;
	}
	
	public static void main(String[] args) throws TSException, Exception{
		List<String> individualPerfIdtsShareSplitRatioList = new ArrayList<String>();
		List<String> individualPerfIdtsSpinoffList = new ArrayList<String>();
		List<String> individualPerfIdtsStockDistributionList = new ArrayList<String>();
		List<String> individualPerfIdtsSpecialCashDividendList = new ArrayList<String>();
		List<String> individualPerfIdtsDividendFrequencyList = new ArrayList<String>();
		List<String> individualPerfIdtsCashDividendList = new ArrayList<String>();
		List<String> individualPerfIdtsCashDividendDatesList = new ArrayList<String>();
		List<String> individualPerfIdtsCashDividendCurrencyHistoryList = new ArrayList<String>();
		List<String> individualPerfIdtsSpecialCashDividendDatesList = new ArrayList<String>();
		List<String> individualPerfIdtsSpecialCashDividendCurrencyHistoryList = new ArrayList<String>();
		List<String> individualPerfIdtsSplitDatesList = new ArrayList<String>();
		List<String> individualPerfIdtsStockDistributionDatesList = new ArrayList<String>();
		List<String> individualPerfIdtsRightsOfferingDatesList = new ArrayList<String>();
		List<String> individualPerfIdtsRightsOfferingList = new ArrayList<String>();
		List<String> individualPerfIdtsRightsOfferingAdjustmentFactorList = new ArrayList<String>();
		List<String> individualPerfIdtsRightsOfferingCurrencyHistoryList = new ArrayList<String>();
		
		String tableName = "DBI_EQUITYPRICECORPACTION";
		String createdOnTime = "2014-06-16";
		int limitCount = 200;
		List<String> performanceIdListEODMode = new ArrayList<String>();

//EOD MODE
		performanceIdListEODMode = getUpdatedPerformanceIdListEODMode(tableName, createdOnTime);
		if(!performanceIdListEODMode.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsShareSplitRatio,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpinoff,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsStockDistribution,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividend,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsDividendFrequency,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividend,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividendDates,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividendCurrencyHistory,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividendDates,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividendCurrencyHistory,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSplitDates,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsStockDistributionDates,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsRightsOfferingDates,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsRightsOffering,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsRightsOfferingAdjustmentFactor,performanceIdListEODMode,limitCount);
			compareUpdatedData(TsBlobDataTypeBaseObject.tsRightsOfferingCurrencyHistory,performanceIdListEODMode,limitCount);
		}else{
			System.out.println("[WARN]PerformanceId(EOD MODE) list is empty,ignore this QA check!");
		}
		
//Individual PerformanceId mode
		individualPerfIdtsShareSplitRatioList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsShareSplitRatio.TsType,limitCount);
		if(!individualPerfIdtsShareSplitRatioList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsShareSplitRatio, individualPerfIdtsShareSplitRatioList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsShareSplitRatio list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsSpinoffList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsSpinoff.TsType,limitCount);
		if(!individualPerfIdtsSpinoffList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpinoff, individualPerfIdtsSpinoffList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsSpinoff list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsSpinoffList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsSpinoff.TsType,limitCount);
		if(!individualPerfIdtsSpinoffList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpinoff, individualPerfIdtsSpinoffList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsSpinoff list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsStockDistributionList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsStockDistribution.TsType,limitCount);
		if(!individualPerfIdtsStockDistributionList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsStockDistribution, individualPerfIdtsStockDistributionList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsStockDistribution list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsSpecialCashDividendList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsSpecialCashDividend.TsType,limitCount);
		if(!individualPerfIdtsSpecialCashDividendList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividend, individualPerfIdtsSpecialCashDividendList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsSpecialCashDividend list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsDividendFrequencyList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsDividendFrequency.TsType,limitCount);
		if(!individualPerfIdtsDividendFrequencyList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsDividendFrequency, individualPerfIdtsDividendFrequencyList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsDividendFrequency list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsCashDividendList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsCashDividend.TsType,limitCount);
		if(!individualPerfIdtsCashDividendList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividend, individualPerfIdtsCashDividendList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsCashDividend list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsCashDividendDatesList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsCashDividendDates.TsType,limitCount);
		if(!individualPerfIdtsCashDividendDatesList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividendDates, individualPerfIdtsCashDividendDatesList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsCashDividendDates list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsCashDividendCurrencyHistoryList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsCashDividendCurrencyHistory.TsType,limitCount);
		if(!individualPerfIdtsCashDividendCurrencyHistoryList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsCashDividendCurrencyHistory, individualPerfIdtsCashDividendCurrencyHistoryList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsCashDividendCurrencyHistory list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsSpecialCashDividendDatesList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsSpecialCashDividendDates.TsType,limitCount);
		if(!individualPerfIdtsSpecialCashDividendDatesList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividendDates, individualPerfIdtsSpecialCashDividendDatesList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsSpecialCashDividendDates list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsSpecialCashDividendCurrencyHistoryList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsSpecialCashDividendCurrencyHistory.TsType,limitCount);
		if(!individualPerfIdtsSpecialCashDividendCurrencyHistoryList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSpecialCashDividendCurrencyHistory, individualPerfIdtsSpecialCashDividendCurrencyHistoryList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsSpecialCashDividendCurrencyHistory list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsSplitDatesList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsSplitDates.TsType,limitCount);
		if(!individualPerfIdtsSplitDatesList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsSplitDates, individualPerfIdtsSplitDatesList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsSplitDates list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsStockDistributionDatesList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsStockDistributionDates.TsType,limitCount);
		if(!individualPerfIdtsStockDistributionDatesList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsStockDistributionDates, individualPerfIdtsStockDistributionDatesList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsStockDistributionDates list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsRightsOfferingDatesList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsRightsOfferingDates.TsType,limitCount);
		if(!individualPerfIdtsRightsOfferingDatesList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsRightsOfferingDates, individualPerfIdtsRightsOfferingDatesList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsRightsOfferingDates list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsRightsOfferingList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsRightsOffering.TsType,limitCount);
		if(!individualPerfIdtsRightsOfferingList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsRightsOffering, individualPerfIdtsRightsOfferingList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsRightsOffering list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsRightsOfferingAdjustmentFactorList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsRightsOfferingAdjustmentFactor.TsType,limitCount);
		if(!individualPerfIdtsRightsOfferingCurrencyHistoryList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsRightsOfferingAdjustmentFactor, individualPerfIdtsRightsOfferingAdjustmentFactorList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsRightsOfferingAdjustmentFactor list is empty,ignore this QA check!");
		}
		
		individualPerfIdtsRightsOfferingCurrencyHistoryList = getUpdatedPerformanceIdListIndividualMode(tableName,TsBlobDataTypeBaseObject.tsRightsOfferingCurrencyHistory.TsType,limitCount);
		if(!individualPerfIdtsRightsOfferingCurrencyHistoryList.isEmpty()){
			compareUpdatedData(TsBlobDataTypeBaseObject.tsRightsOfferingCurrencyHistory, individualPerfIdtsRightsOfferingCurrencyHistoryList,limitCount);
		}else{
			System.out.println("[WARN]Individual PerfId TsRightsOfferingCurrencyHistory list is empty,ignore this QA check!");
		}
	}
}
