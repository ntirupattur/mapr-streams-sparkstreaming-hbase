/**
 * 
 */
package com.mapr.spyglass.solution;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author ntirupattur
 * Class with static methods to generate hashes for individual strings or tags
 *
 */
public class StringsUtil {

	public static String getHashForTags(String tags) throws Exception {
		List<String> tagsList = getTagsList(tags);
		StringBuffer hashForTags = new StringBuffer();
		for (String tag: tagsList) {
			hashForTags.append(getHash((tag.trim())));
			hashForTags.append("|");
		}
		return hashForTags.substring(0, hashForTags.length()-1).toString();
	}

	public static List<String> getTagsList(String tags) {
		List<String> tagsList = Arrays.asList(tags.replaceAll("\\{|\\}", "").split("\\s*,\\s*"));

		if (tagsList.size() > 0) {
			tagsList.sort(new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}
			});
		}
		return tagsList;
	}
	
	public static String getRegexForTags(String tags) throws Exception {
		List<String> tagsList = getTagsList(tags);
		StringBuffer hashForTags = new StringBuffer();
		for (String tag: tagsList) {
			hashForTags.append(getHash((tag.trim())));
			hashForTags.append(".+");
		}
		return hashForTags.substring(0, hashForTags.length()-2).toString();
	}

	public static StringBuffer getHash(String tag) throws Exception {
		MessageDigest messageDigest;
		StringBuffer hashString = new StringBuffer();
		try {
			messageDigest = MessageDigest.getInstance("MD5");
			messageDigest.update(tag.getBytes());
			byte[] messageDigestMD5 = messageDigest.digest();
			for (byte bytes : messageDigestMD5) {
				hashString.append(String.format("%02x", bytes & 0xff));
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return hashString;
	}
}
