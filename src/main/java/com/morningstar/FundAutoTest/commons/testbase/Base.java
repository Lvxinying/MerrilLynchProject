package com.morningstar.FundAutoTest.commons.testbase;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeMethod;

import com.ibm.icu.text.SimpleDateFormat;
import com.morningstar.FundAutoTest.commons.ResourceManager;

public class Base {
//变量声明，初始值均为“空”的字符串,caseResult为true,logger为一个LOG类变量	
	public static String runFrequency = "";
	public static String baseUrl = "";
	public static String token = "";
	public static String email = "";
	public static String liveSite = "";
	public static String QASite = "";
	public static String UATSite = "";
	public static String DevSite = "";
	protected String caseName = "";
	public static String siteTag = "";
	public static boolean caseResult = true;
	private static final Logger logger = LoggerFactory.getLogger(Base.class);
	
	//init all variables from configuration file
	static {				
		liveSite = ResourceManager.getLiveSite();
		QASite = ResourceManager.getQASite();
		UATSite = ResourceManager.getUATSite();
		DevSite = ResourceManager.getDevSite();
	}
	
	//determine baseurl before test start
	@BeforeSuite
	public void setBaseURL() {
		if (isLive())
		{
			baseUrl = liveSite;
			token = ResourceManager.gettoken(); 
		}			
		if (isQA())
		{
			baseUrl = QASite;
			token = ResourceManager.getQAToken();
		}
			
		if (isUAT())
		{
			baseUrl = UATSite;
			token = ResourceManager.gettoken(); 
		}	
			
		if (isDev())
		{
			baseUrl = DevSite;
			token = ResourceManager.getDevToken();
		}		
	}
//加载类之前运行
	@BeforeClass
	public void setUp() throws Exception {
		caseName = this.getClass().getName();
		logger.info("+++++++++++++++++++++++++++++++++++" + caseName
				+ "+++++++++++++++++++++++++++++++++++");
	}
//加载方法之前运行	
	@BeforeMethod
	public void setUpMethod() throws Exception {
		caseResult = true;
	}
/*	
	@AfterMethod
	public void tearDownMethod() throws Exception {
		Assert.assertTrue(caseResult);
	}
*/
	public static boolean isLive() {
		return "Live".equals(Base.siteTag);
	}

	public static boolean isQA() {
		return "QA".equals(Base.siteTag);
	}

	public static boolean isUAT() {
		return "UAT".equals(Base.siteTag);
	}
	
	public static boolean isDev() {
		return "Dev".equals(Base.siteTag);
	}

	public static String currentSysTime(){
		String currenTime = null;
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		try {
			String Time = df.format(new Date());
			currenTime = Time;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return currenTime;		
	}
}
