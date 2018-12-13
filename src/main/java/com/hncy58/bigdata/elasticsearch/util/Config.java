package com.hncy58.bigdata.elasticsearch.util;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	private static Logger logger = LoggerFactory.getLogger(Config.class);
	private final static Map<String, Object> configValue = new HashMap<String, Object>();

	static {
		String fileName = "/config/config.xml";
		URL resource = Config.class.getResource(fileName);
		if (resource == null) {
			resource = Config.class.getClassLoader().getResource(fileName);
			if (resource == null) {
				resource = ClassLoader.getSystemResource(fileName);
			}
		}
	    Config.start(resource.getPath());
	}

	/**
	 * 配置文件的检测时间间隔
	 */
	private static long interval = 5000L;

	/**
	 * 配置文件的上次修改时间
	 */
	private static long lastModified = 0L;

	/**
	 * 检测线程
	 */
	private static Thread detectThread;
	
	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的值
	 */
	public static Object getValue(String xmlPath){
		return configValue.get(xmlPath);
	}

	/**
	 * 设置(修改)配置文件中key所对应的值<br>修改并不会保存到文件
	 * @param key 节点路径
	 * @param value 节点路径对应的值
	 */
	public static void setValue(String key, Object value){
		configValue.put(key, value);
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的String值
	 */
	public static String getStringValue(String xmlPath){
		return (String)getValue(xmlPath);
	}
	
	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @param defaultValue 无有效值的默认值
	 * @return 节点所对应的String值
	 */
	public static String getStringValue(String xmlPath, String defaultValue){
		String result = defaultValue;
		String value = getStringValue(xmlPath);
		if (value != null)
			result = value;
		return result;
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的Integer值
	 */
	public static Integer getIntegerValue(String xmlPath){
		return (Integer)getValue(xmlPath);
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @param defaultValue 无有效值的默认值
	 * @return 节点所对应的int值
	 */
	public static int getIntValue(String xmlPath, int defaultValue){
		int result = defaultValue;
		Integer value = getIntegerValue(xmlPath);
		if(value != null)
			result = value;
		return result;
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的int值
	 * @throws Exception 节点未包含有效整数抛出异常
	 */
	public static int getIntValue(String xmlPath) throws Exception{
		Integer value = getIntegerValue(xmlPath);
		if(value != null)
			return value.intValue();
		else
			throw new Exception("配置文件 " + xmlPath + " 未包含有效整数!");
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的Long值
	 */
	public static Long getLongValue(String xmlPath){
		return (Long)getValue(xmlPath);
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @param defaultValue 无有效值的默认值
	 * @return 节点所对应的long值
	 */
	public static long getLongValue(String xmlPath, long defaultValue){
		long result = defaultValue;
		Long value = getLongValue(xmlPath);
		if(value != null)
			result = value;
		return result;
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的Float值
	 */
	public static Float getFloatValue(String xmlPath){
		return (Float)getValue(xmlPath);
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @param defaultValue 无有效值的默认值
	 * @return 节点所对应的float值
	 */
	public static float getFloatValue(String xmlPath, float defaultValue){
		float result = defaultValue;
		Float value = getFloatValue(xmlPath);
		if(value != null)
			result = value;
		return result;
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的Double值
	 */
	public static Double getDoubleValue(String xmlPath){
		return (Double)getValue(xmlPath);
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @param defaultValue 无有效值的默认值
	 * @return 节点所对应的double值
	 */
	public static double getDoubleValue(String xmlPath, double defaultValue){
		double result = defaultValue;
		Double value = getDoubleValue(xmlPath);
		if(value != null)
			result = value;
		return result;
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的Boolean值
	 */
	public static Boolean getBooleanValue(String xmlPath){
		return (Boolean)getValue(xmlPath);
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @param defaultValue 无有效值的默认值
	 * @return 节点所对应的Boolean值
	 */
	public static Boolean getBooleanValue(String xmlPath, Boolean defaultValue){
		Boolean result = defaultValue;
		Boolean value = getBooleanValue(xmlPath);
		if(value != null)
			result = value;
		return result;
	}

	/**
	 * 取得配置文件中的xmlPath所指的值
	 * @param xmlPath XML配置文件中的节点路径
	 * @return 节点所对应的Date值
	 */
	public static Date getDateValue(String xmlPath){
		return (Date)getValue(xmlPath);
	}

	/**
	 * 读取配置
	 * @param configFile 配置文件
	 */
	private static void setConfig(File configFile){
		if(!configFile.exists()){
			logger.error("配置文件 {} 不存在。", configFile.getAbsolutePath());
			return;
		}

		if(configFile.lastModified() > lastModified){
			lastModified = configFile.lastModified();

			try {
				SAXReader reader = new SAXReader();
				Document doc = reader.read(configFile);

				Element root = doc.getRootElement();
				readElementValue(root);
			} catch (Exception e) {
				logger.error("读取配置文件出错: ", e);
			}
		}
	}
	
	/**
	 * 读取配置
	 * @param configFile 配置文件
	 */
	private static void setConfig(InputStream configFile){
		try {
			SAXReader reader = new SAXReader();
			Document doc = reader.read(configFile);

			Element root = doc.getRootElement();
			readElementValue(root);
		} catch (Exception e) {
			logger.error("读取配置文件出错: ", e);
		}
	}
	
	protected static boolean readSingleElementValue(Element element){
		boolean result = false;
		Attribute attribute = element.attribute("type");

		String type = null;
		if(attribute != null){
			type = attribute.getValue();
		} else {
			type = "String";
		}

		attribute = element.attribute("description");
		String description;
		if(attribute != null){
			description = attribute.getValue();
		} else {
			description = "";
		}

		String key = element.getPath();
		String val;

		attribute = element.attribute("value");
		if(attribute != null){
			val = attribute.getValue();
		} else {
			val = (String)element.getText();
		}

		Object value = null;
		try {
			value = readValue(type, val);
		} catch (Exception e) {
			logger.error("{} {} 期望{}值,实际:\"{}\", 错误: {}", key, description, type, val, e.getMessage());
		}

		Object oldValue = configValue.get(key);
		if(oldValue == null && value != null
				|| oldValue != null && value == null
				|| value != null && value.equals(oldValue) == false){
			synchronized(Config.class){
				configValue.put(key, value);
			}
			result = true;
			if(oldValue == null)
				logger.info("{} {} 初使化为: {}", key, description, value);
			else
				logger.info("{} {} 改变: {} -> {}", key, description, oldValue, value);

			//"/config/interval"这个部分用来调整配置文件的检测间隔时间
			if("/config/interval".equals(key)){
				Config.interval = 1000L * ((Integer)value).intValue();
			}
		}
		return result;
	}
	
	@SuppressWarnings("unchecked")
	protected static void readMultiElementValue(Element element){
		List<Element> elements = element.elements();
		for (int i = 0; i < elements.size(); i++) {
			Element subElement = elements.get(i);
			try {
				readElementValue(subElement);
			} catch (Exception e) {
				logger.error("读取值时出现错误: ", e);
			}
		}
	}

	protected static void readElementValue(Element element){
		if(element.nodeCount() > 1){
			readMultiElementValue(element);
		} else {
			readSingleElementValue(element);
		}
	}

	/**
	 * @param type 类型名称(String, Integer, Float, Double, Date)
	 * @param val 字符串值
	 * @return 字符串所表示的实际对象值
	 * @throws ParseException
	 */
	private static Object readValue(String type, String val) throws ParseException {
		Object value = null;
		switch (type.codePointAt(0)) {
		case 0x42: // Boolean
			value = Boolean.valueOf(val);
			break;

		case 0x44: // Date & Double
			if(type.length() == 4){
				value = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(val);
			} else {
				value = Double.valueOf(val);
			}
			break;

		case 0x46: // Float
			value = Float.valueOf(val);
			break;

		case 0x49: // Integer
			value = Integer.valueOf(val);
			break;

		case 0x4C: // Long
			value = Long.valueOf(val);
			break;

		default: // String
			value = val;
			break;
		}
		return value;
	}
	
	/**
	 * 启动一个线程，每间隔interval检测一次配置文件的变化
	 * @param filePath 配置文件全路径
	 * @param interval 检测间隔时间(单位：秒)
	 */
	public static void start(final String filePath){
		final File configFile = new File(filePath);

		if (!configFile.exists()) {
			System.out.println("配置文件不存在在，使用流");
			InputStream is = Config.class.getResourceAsStream("/config/config.xml");
			setConfig(is);
			return;
		}

		setConfig(configFile);

		detectThread = new Thread(){
			public void run(){
				logger.info("配置文件检测线程启动");
				while(true){
					try {
						Thread.sleep(Config.interval);
					} catch (InterruptedException e) {
						break;
					}

					setConfig(configFile);
				}
				logger.info("配置文件检测线程停止");
			}
		};
		detectThread.setDaemon(true);
		detectThread.start();
	}
}
