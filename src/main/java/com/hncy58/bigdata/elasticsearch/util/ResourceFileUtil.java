package com.hncy58.bigdata.elasticsearch.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在当前classpath所有文件系统的路径下去找指定名称的资源文件<br>
 * 直接不能找到的时候，使用递归查找。
 * 更复杂的查找建议使用Spring的实现（多个JAR中的文件）：new org.springframework.core.io.support.PathMatchingResourcePatternResolver().getResources("classpath*:**&#x2f;MANIFEST.MF") 
 * @author luodongshan
 * @date 2018年6月9日 上午9:24:41
 *
 */
public class ResourceFileUtil {
	/** 日志工具 */
	private static Logger logger = LoggerFactory.getLogger(ResourceFileUtil.class);

	private String resourceName;

	public ResourceFileUtil(String resourceName) {
		this.resourceName = resourceName;
	}

	/**
	 * 递归查找，直到找到
	 * @param dir 目录
	 * @return 找到的文件，否则为null
	 */
	protected File doRetrieveSearchingFiles(File dir) {
		logger.trace("Searching directory [" + dir.getAbsolutePath() + "]");
		File[] dirContents = dir.listFiles();
		if (dirContents == null) {
			logger.warn("Could not retrieve contents of directory [" + dir.getAbsolutePath() + "]");
			return null;
		}

		for (File content : dirContents) {
			String currPath = replace(content.getAbsolutePath());
			if (content.isDirectory()) {
				if (!content.canRead()) {
					logger.debug("Skipping subdirectory [" + dir.getAbsolutePath() + "] because the application is not allowed to read the directory");
				} else {
					File file;
					if ((file = doRetrieveSearchingFiles(content)) != null) {
						return file;
					}
				}
			}

			logger.trace("{}", currPath);
			if (currPath.endsWith(resourceName)) {
				return content;
			}
		}

		return null;
	}

	/**
	 * 列当前classpath的所有根目录
	 */
	protected List<File> listRootDirResources() {
		try {
			ClassLoader cl = this.getClass().getClassLoader();
			Enumeration<URL> resourceUrls = (cl != null ? cl.getResources("") : ClassLoader.getSystemResources(""));
			List<File> result = new ArrayList<File>(16);
			while (resourceUrls.hasMoreElements()) {
				URL url = resourceUrls.nextElement();
				result.add(new File(url.getFile()));
			}
			return result;
		} catch (IOException e) {
			return Collections.emptyList();
		}
	}

	/**
	 * 查找指定名字的资源文件
	 * @param resourceName 资源文件名
	 * @return 具体文件，找不到时为null
	 */
	public static File getResourceFile(String resourceName) {
		resourceName = replace(resourceName);
		URL url = getResource(resourceName);
		if (url != null) {
			return new File(url.getFile());
		}

		ResourceFileUtil resourceUtil = new ResourceFileUtil(resourceName);
		List<File> rootDirResources = resourceUtil.listRootDirResources();
		File file;
		for (File dir : rootDirResources) {
			if ((file = resourceUtil.doRetrieveSearchingFiles(dir)) != null) {
				return file;
			}
		}
		return null;
	}

	/**
	 * 直接查找
	 */
	private static URL getResource(String fileName) {
		URL resource = ResourceFileUtil.class.getResource(fileName);
		if (resource == null) {
			resource = ResourceFileUtil.class.getClassLoader().getResource(fileName);
			if (resource == null) {
				resource = ClassLoader.getSystemResource(fileName);
			}
		}
		return resource;
	}

	/**
	 * 将\替换为/
	 */
	public static String replace(String inString) {
		if (inString == null || inString.length() == 0) {
			return inString;
		}

		String oldPattern = File.separator, newPattern = "/";

		StringBuilder sb = new StringBuilder();
		int pos = 0; // our position in the old string
		int index = inString.indexOf(oldPattern);
		// the index of an occurrence we've found, or -1
		int patLen = oldPattern.length();
		while (index >= 0) {
			sb.append(inString.substring(pos, index));
			sb.append(newPattern);
			pos = index + patLen;
			index = inString.indexOf(oldPattern, pos);
		}
		sb.append(inString.substring(pos));
		// remember to append any characters to the right of a match
		return sb.toString();
	}
}