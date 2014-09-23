package com.morningstar.FundAutoTest.commons;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class test {
	 public static void main(String args[]){
	        Map<Person,String> map = null ; // 声明Map对象 
	        map = new IdentityHashMap<Person,String>() ; 
	        map.put(new Person("IG001DA013"),"0.0275") ; // 加入内容 
	        map.put(new Person("IG001DA013"),"0.0276") ; // 加入内容 
	        map.put(new Person("IG001DA017"),"0.0276") ;   // 加入内容 
	        Set<Map.Entry<Person,String>> allSet = null ;   // 准备使用Set接收全部内容 
	        allSet = map.entrySet() ; 
	        Iterator<Map.Entry<Person,String>> iter = null ; 
	        iter = allSet.iterator() ; 
	        while(iter.hasNext()){ 
	            Map.Entry<Person,String> me = iter.next() ; 
	            System.out.println(me.getKey() + " --> " + me.getValue()) ; 
	        } 
	  } 

}



class Person{ 
    private String name ; 
    public Person(String name){ 
        this.name = name ; 
    } 
    public boolean equals(Object obj){ 
        if(this==obj){ 
            return true ; 
        } 
        if(!(obj instanceof Person)){ 
            return false ; 
        } 
        Person p = (Person)obj ; 
        if(this.name.equals(p.name)){ 
            return true ; 
        }else{ 
            return false ; 
        } 
    } 
    public int hashCode(){ 
        return this.name.hashCode(); 
    } 
    public String toString(){ 
        return this.name; 
    } 
}; 
