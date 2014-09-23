package com.morningstar.Fund.TestRunner;
import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;


public class TestNGSample1 {
	@DataProvider(name = "range-provider")
	public Object[][] rangeData() {
	int lower = 5;
	int upper = 10;
	return new Object[][] {
			{ lower-1, lower, upper, false },
			{ lower, lower, upper, true },
			{ lower+1, lower, upper, true },
			{ upper, lower, upper, true},
			{ upper+1, lower, upper, false },
		};
	}
	
	@Test(dataProvider = "range-provider")
	public void testIsBetween(int n, int lower,int upper, boolean expected)
	{
		System.out.println("Received " + n + " " + lower + "-"+ upper + " expected: " + expected);
//		Assert.assertEquals(expected, isBetween(n, lower, upper));
	}
}
