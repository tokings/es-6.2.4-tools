package com.hncy58.bigdata.elasticsearch.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;


/**
 * 日期类型常用工具类
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月17日 下午4:12:10
 *
 */
public class DateUtil 
{
	/**
	 * 年月日缺省分隔符
	 */
	private static char DAY_DELIMITER = '-';
	
	/**
	 * 
	 * @return
	 */
	public static String getDate()
	{
		Calendar calendar = Calendar.getInstance(Locale.CHINA);		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(calendar.getTime());
	}
	
	/**
	 * 
	 * @return
	 */
	public static Calendar getFirstDayOfMonth() 
	{
		Calendar calendar = Calendar.getInstance(Locale.CHINA);
		calendar.set(Calendar.DAY_OF_MONTH, 1);		
		return calendar;
	}

	/**
	 * 
	 * @return
	 */
	public static Calendar getFirstDayOfNextMonth() 
	{
		Calendar calendar = Calendar.getInstance(Locale.CHINA);
		calendar.add(Calendar.MONTH, 1);
		calendar.set(Calendar.DAY_OF_MONTH, 1);		
		return calendar;
	}
	
	/**
	 * 
	 * @return
	 */
	public static Calendar getMiddleDayOfNextMonth() 
	{
		Calendar calendar = Calendar.getInstance(Locale.CHINA);
		calendar.add(Calendar.MONTH, 1);	
		calendar.set(Calendar.DAY_OF_MONTH, 15);
		return calendar;
	}
	
	/**
	 * 
	 * @return
	 */
	public static Calendar getMiddleDayOfMonth() 
	{
		Calendar calendar = Calendar.getInstance(Locale.CHINA);
		calendar.set(Calendar.DAY_OF_MONTH, 15);		
		return calendar;
	}
	
	/**
	 * 
	 * @return
	 */
	public static Calendar getLastDayOfMonth() 
	{
		Calendar calendar = Calendar.getInstance(Locale.CHINA);
		int i = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
		calendar.set(Calendar.DAY_OF_MONTH, i);
		return calendar;
	}
	
	/**
	 * 
	 * @param offset
	 * @param format
	 * @return
	 */
	public static String getDayOffset(int offset, String format) 
	{
		Calendar calendar = Calendar.getInstance(Locale.CHINA);
		calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + offset);		
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(calendar.getTime());
	}

	/**
	 * 
	 * @param date
	 * @param offset
	 * @param format
	 * @return
	 */
	public static String getDayOffset(String date, int offset, String format) 
	{
		Calendar calendar = Calendar.getInstance(Locale.CHINA);
		calendar.setTime(toDate(date));		
		calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + offset);		
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(calendar.getTime());
	}

	/**
	 * 
	 * @param date
	 * @return
	 */
	public static String toString(Date date) 
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(date);
	}

	/**
	 * 
	 * @param date
	 * @param format
	 * @return
	 */
	public static String toString(Date date, String format) 
	{
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
	}
	
	/**
	 * 
	 * @param date
	 * @return
	 */
	public static Date toDate(String date)
	{
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			if (date.length() == 10)
				sdf = new SimpleDateFormat("yyyy-MM-dd");
			return sdf.parse(date);
		} catch (ParseException pe) {
			throw new RuntimeException(pe);
		}
	}

	/**
	 * 
	 * @param date
	 * @param format
	 * @return
	 */
	public static Date toDate(String date, String format)
	{
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return sdf.parse(date);
		} catch (ParseException pe) {
			throw new RuntimeException(pe);
		}
	}
	
	/**
	 * 
	 * @param type
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static long diff(int type, Date date1, Date date2) 
	{
		switch(type) {
			case Calendar.YEAR:
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(date1);
				long time = calendar.get(Calendar.YEAR);
				calendar.setTime(date2);
				return time - calendar.get(Calendar.YEAR);
	
			case Calendar.DATE:
				time = date1.getTime() / 1000 / 60 / 60 / 24;
				return time - date2.getTime() / 1000 / 60 / 60 / 24;
	
			case Calendar.HOUR:
				time = date1.getTime() / 1000 / 60 / 60;
				return time - date2.getTime() / 1000 / 60 / 60;
	
			case Calendar.SECOND:
				time = date1.getTime() / 1000;
				return time - date2.getTime() / 1000;
		}
		return date1.getTime() - date2.getTime();
	}
	
	/**
	 * 取得系统默认时区的日期时间
	 * 
	 * @return String YYYY-MM-DD HH:MM:DD
	 */
	public static String getDateTime() {
		return getDateTime(new GregorianCalendar());
	}
	
	/**
	 * 根据日历返回日期时间
	 * 
	 * @param Calendar
	 *            日历
	 * @return String YYYY-MM-DD HH:MM:DD
	 */
	private static String getDateTime(Calendar calendar) {
		StringBuffer buf = new StringBuffer("");

		buf.append(calendar.get(Calendar.YEAR));
		buf.append(DAY_DELIMITER);
		buf.append(calendar.get(Calendar.MONTH) + 1 > 9 ? calendar
				.get(Calendar.MONTH)
				+ 1 + "" : "0" + (calendar.get(Calendar.MONTH) + 1));
		buf.append(DAY_DELIMITER);
		buf.append(calendar.get(Calendar.DAY_OF_MONTH) > 9 ? calendar
				.get(Calendar.DAY_OF_MONTH)
				+ "" : "0" + calendar.get(Calendar.DAY_OF_MONTH));
		buf.append(" ");
		buf.append(calendar.get(Calendar.HOUR_OF_DAY) > 9 ? calendar
				.get(Calendar.HOUR_OF_DAY)
				+ "" : "0" + calendar.get(Calendar.HOUR_OF_DAY));
		buf.append(":");
		buf.append(calendar.get(Calendar.MINUTE) > 9 ? calendar
				.get(Calendar.MINUTE)
				+ "" : "0" + calendar.get(Calendar.MINUTE));
		buf.append(":");
		buf.append(calendar.get(Calendar.SECOND) > 9 ? calendar
				.get(Calendar.SECOND)
				+ "" : "0" + calendar.get(Calendar.SECOND));
		return buf.toString();
	}
	
	/**
	 * 将时间转为formate格式
	 * @param date
	 * @param formate
	 * @return
	 */
	public static String dateToString(long date, String formate){
		return dateToString(new Date(date), formate);
	}
	
	/**
	 * 将一Date类型的对象，转换为一个字符串
	 * @param format 默认"yyyy-MM-dd HH:mm:ss.SSS"
	 */
	public static String dateToString(Date date, String formate){
		if( date==null ){
			return "";
		}
		formate = (formate==null) ? "yyyy-MM-dd HH:mm:ss.SSS" : formate;
		return new SimpleDateFormat(formate).format(date);
	}
	
	/**
	 * 将一Date类型的对象，转换为一个 "1998-01-01 01:01:01" 这样的字符串
	 */
	public static String dateToString(Date date){
		return dateToString(date, "yyyy-MM-dd HH:mm:ss");
	}
	
	public static String dateToString(long date){
		return dateToString(new Date(date));
	}
	
	public static String getDayAfter(String day, int filed, int delta) 
	{  
        Calendar c = Calendar.getInstance();  
        
        Date date = null;  
        
        try {  
            date = new SimpleDateFormat("yy-MM-dd HH:mm:ss").parse(day);  
        } catch (ParseException e) {  
        }  
        
        c.setTime(date);  
        
        int iday = c.get(filed);  
        
        c.set(filed, iday + delta);  
  
        String dayAfter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());  
        return dayAfter;  
    }  
	
	public static String getDayAfter(String dateformat, String day, int filed, int delta) 
	{  
		Calendar c = Calendar.getInstance();  
		
		Date date = null;  
		
		try {  
			date = new SimpleDateFormat(dateformat).parse(day);  
		} catch (ParseException e) {  
		}  
		
		c.setTime(date);  
		
		int iday = c.get(filed);  
		
		c.set(filed, iday + delta);  
		
		String dayAfter = new SimpleDateFormat(dateformat).format(c.getTime());  
		return dayAfter;  
	}  
	
	public static String getDayAfter(Date day, int filed, int delta)
	{
		return getDayAfter(dateToString(day), filed, delta);
	}
	
	public static String getDayAfter(String dateformat, Date day, int filed, int delta)
	{
		return getDayAfter(dateformat, dateToString(day, dateformat), filed, delta);
	}
	/**
	 * 时间转星期
	 * @param dt
	 * @return
	 */
	public static String getWeekOfDate(Date dt) {
        String[] weekDays = {"7", "1", "2", "3", "4", "5", "6"};
        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if (w < 0)
            w = 0;
        return weekDays[w];
    }
	
	/** 
     * 获取某天的开始时间到当天的结束时间
     * @param date 当前时间 
     * @flag 0 返回yyyy-MM-dd 00:00:00日期<br> 
     *       1 返回yyyy-MM-dd 23:59:59日期 
     * @return 
     */  
    public static Date weeHours(Date date, int flag) {  
        Calendar cal = Calendar.getInstance();  
        cal.setTime(date);  
        int hour = cal.get(Calendar.HOUR_OF_DAY);  
        int minute = cal.get(Calendar.MINUTE);  
        int second = cal.get(Calendar.SECOND);  
        //时分秒（毫秒数）  
        long millisecond = hour*60*60*1000 + minute*60*1000 + second*1000;  
        //凌晨00:00:00  
        cal.setTimeInMillis(cal.getTimeInMillis()-millisecond);  
           
        if (flag == 0) {  
            return cal.getTime();  
        } else if (flag == 1) {  
            //凌晨23:59:59  
            cal.setTimeInMillis(cal.getTimeInMillis()+23*60*60*1000 + 59*60*1000 + 59*1000);  
        }  
        return cal.getTime();  
    } 
    
    /**
     * 获取明天凌晨1点钟时间
     * @return
     */
    public static Date getNextDay1Hour() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR_OF_DAY, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.add(Calendar.DAY_OF_MONTH, 1);
        return cal.getTime();
    }

	public static String[] getDateStrSectionByDay(String format, String startTime, String endTime) {

		List<String> fields = new ArrayList<>();
		Calendar start = Calendar.getInstance();
		start.setTimeInMillis(DateUtil.toDate(startTime).getTime());
		Calendar end = Calendar.getInstance();
		end.setTimeInMillis(DateUtil.toDate(endTime).getTime());

		while (!end.before(start)) {
			String field = dateToString(start.getTimeInMillis(), format);
			fields.add(field);
			start.set(Calendar.DAY_OF_MONTH, start.get(Calendar.DAY_OF_MONTH) + 1);
		}

		return fields.toArray(new String[fields.size()]);
	}
	
	public static void main(String[] args) throws ParseException 
	{		
		System.out.println(DateUtil.getWeekOfDate(DateUtil.toDate("2015-08-20","yyyy-MM-dd")));
//		String strDate = "2014-10-12 21:22:11";
//		System.out.println(DateUtil.getDayAfter(strDate, Calendar.HOUR_OF_DAY, 8));
//		System.out.println(DateUtil.dateToString(DateUtil.toDate(strDate), "yyyyMMddHHmmss"));
		/*System.out.println(DateUtil.getDayOffset("2012-02-28", 7, "yyyy-MM-dd"));
		Date current = Calendar.getInstance().getTime();
		
		Date dLastTestDate = DateUtil.toDate("2012-12-28 00:00:00.0");
		System.out.println(DateUtil.diff(Calendar.DATE, current, dLastTestDate));
		
		System.out.println(DateUtil.getHourOffset(-1, "yyyy-MM-dd hh:mm:ss"));
		System.out.println(DateUtil.toDate( DateUtil.getDayOffset(-1, "yyyy-MM-dd") + " 23:59:59" ));
		System.out.println(Calendar.getInstance().get(Calendar.MONTH));
		System.out.println(DateUtil.getFirstDayOfWeek());
		System.out.println(DateUtil.getFirstDayOfMonth());
		System.out.println(DateUtil.getLastDayOfMonth());
		System.out.println(DateUtil.getWeekOfYear());
		System.out.println(DateUtil.getThreeMonthsBeforeDate());
		*/
	}
	
}
