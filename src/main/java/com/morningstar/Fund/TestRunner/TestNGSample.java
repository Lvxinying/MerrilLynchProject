package com.morningstar.Fund.TestRunner;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.lang.reflect.Method;
import org.testng.annotations.DataProvider;

public class TestNGSample {
	@DataProvider(name = "inputData")
	public Object[][] dp(Method method)
	{ 
		Object[][] result =null;
		if(method.getName().equalsIgnoreCase("getShuXiang"))
		{   
			result=new Object[][]
			{ 
				{2012,"龙"}, 
				{2011,"兔"},
				{1936,"鼠"},
				{1913,"牛"},
				{1986,"虎"},
				{1977,"蛇"},
				{1930,"马"},
				{1907,"羊"},
				{1968,"猴"},
				{1993,"鸡"},
				{2007,"猪"},
				{2018,"狗"},
				{-2,"error"}
			};   
		}
		else if(method.getName().equalsIgnoreCase("getShuList"))
			{  
			result=new Object[][]
					{ 
					{2000,2012,"猪",2007}, 
					{1993,2000,"鼠",1996}, 
					{1969,1974,"鸡",1969} , 
					{2069,2059,"兔",2059}
					};  
			}
			return result;
	}
	
	  @Test(dataProvider = "inputData")//指定测试方法的参数集合
	  public void getShuXiang(int value,String expected) {
		  System.out.println("年: " + value + " ++++++ 期望属相: "+expected);
	  }
	  
	  @Test(dataProvider = "inputData")//指定测试方法的参数集合
	  public void getShuList(int begin,int end,String shu,int expected) {
//		  TestNGSample ex = new TestNGSample();
		  System.out.println("开始值： "+begin+" 结束值： "+end+" 鼠： "+shu+" 期望值： "+expected);
//		  Assert.assertEquals(lst, ex.getShuList(begin,end,shu, expected));//断言,判定预期结果与实际值是否相等
	  }	 
}
