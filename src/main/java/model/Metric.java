/**
 * 
 */
package model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author ntirupattur
 *
 */
public class Metric {


  public Metric() {}

  public String metricName;
  public long timeStamp;
  public double value;
  public Map<String,String> tags;


  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(metricName).append(" ");
    sb.append(timeStamp).append(" ");
    sb.append(value).append(" ");
    Iterator<Entry<String, String>> iter = tags.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<String, String> entry = iter.next();
      sb.append(entry.getKey());
      sb.append('=');
      sb.append(entry.getValue());
      sb.append(" ");
    }

    return sb.toString();
  }

  /**
   * Converts the string array to a Metric object. WARNING: This method
   * does not perform validation. 
   * @param words The array of strings representing a data point
   * @return An incoming metric object.
   */
  public static Metric getMetricFromString(final String[] words) {
    final Metric dp = new Metric();

    dp.setMetricName(words[1]);
    dp.setTimeStamp(Long.valueOf(words[2]));
    dp.setValue(Double.valueOf(words[3]));

    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = 4; i < words.length; i++) {
      if (!words[i].isEmpty()) {
        parse(tags, words[i]);
      }
    }
    dp.setTags(tags);
    return dp;
  }

  /**
   * Optimized version of {@code String#split} that doesn't use regexps.
   * This function works in O(5n) where n is the length of the string to
   * split.
   * @param s The string to split.
   * @param c The separator to use to split the string.
   * @return A non-null, non-empty array.
   */
  public static String[] splitString(final String s, final char c) {
    final char[] chars = s.toCharArray();
    int num_substrings = 1;
    for (final char x : chars) {
      if (x == c) {
        num_substrings++;
      }
    }
    final String[] result = new String[num_substrings];
    final int len = chars.length;
    int start = 0;  // starting index in chars of the current substring.
    int pos = 0;    // current index in chars.
    int i = 0;      // number of the current substring.
    for (; pos < len; pos++) {
      if (chars[pos] == c) {
        result[i++] = new String(chars, start, pos - start);
        start = pos + 1;
      }
    }
    result[i] = new String(chars, start, pos - start);
    return result;
  }

  /**
   * Parses a tag into a HashMap.
   * @param tags The HashMap into which to store the tag.
   * @param tag A String of the form "tag=value".
   * @throws IllegalArgumentException if the tag is malformed.
   * @throws IllegalArgumentException if the tag was already in tags with a
   * different value.
   */
  public static void parse(final HashMap<String, String> tags,
                           final String tag) {
    final String[] kv = splitString(tag, '=');
    if (kv.length != 2 || kv[0].length() <= 0 || kv[1].length() <= 0) {
      throw new IllegalArgumentException("invalid tag: " + tag);
    }
    if (kv[1].equals(tags.get(kv[0]))) {
        return;
    }
    if (tags.get(kv[0]) != null) {
      throw new IllegalArgumentException("duplicate tag: " + tag
                                         + ", tags=" + tags);
    }
    tags.put(kv[0], kv[1]);
  }
  
  /**
   * @return the metricName
   */
  public String getMetricName() {
    return metricName;
  }

  /**
   * @param metricName the metricName to set
   */
  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  /**
   * @return the timeStamp
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * @param timeStamp the timeStamp to set
   */
  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  /**
   * @return the value
   */
  public double getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue(double value) {
    this.value = value;
  }

  /**
   * @return the tags
   */
  public Map<String, String> getTags() {
    return tags;
  }

  /**
   * @param tags the tags to set
   */
  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

}
