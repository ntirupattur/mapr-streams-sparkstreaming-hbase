package com.mapr.spyglass.dao;

import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.Logger;
import org.ojai.Document;
import org.ojai.json.Json;
import org.ojai.store.DocumentMutation;

import com.mapr.db.Admin;
import com.mapr.db.FamilyDescriptor;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;
import com.mapr.spyglass.model.Metric;
import com.mapr.spyglass.solution.DateTime;
import com.mapr.spyglass.solution.StringsUtil;
import com.tdunning.math.stats.FloatHistogram;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

public class MetricsDao implements java.io.Serializable {

	private static final long serialVersionUID = 1604632922312269888L;
	private static final Logger log = Logger.getLogger(MetricsDao.class);
	private Table table;
	/**
	 * @return the table
	 */
	public Table getTable() {
		return table;
	}

	private static String[] days = {"SUNDAY","MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY"};

	public MetricsDao(String tablePath) throws Exception {
		this.table = createTable(tablePath);
	}

	public Table createTable(String tablePath) throws IOException {
		if (!MapRDB.tableExists(tablePath) ) {

			Admin admin = MapRDB.newAdmin();

			// Create a table descriptor
			TableDescriptor tableDescriptor = MapRDB.newTableDescriptor()
					.setPath(tablePath)  // set the Path of the table in MapR-FS
					.setSplitSize(512)    // Size in mebibyte (Mega Binary Bytes)
					.setBulkLoad(false);   // Created with Bulk mode by default

			// Configuration of the default Column Family, used to store JSON element by default
			FamilyDescriptor familyDesc = MapRDB.newDefaultFamilyDescriptor()
					.setCompression(FamilyDescriptor.Compression.LZF)
					.setInMemory(true); // To tell the DB to keep these value in RAM as much as possible
			tableDescriptor.addFamily(familyDesc);

			table = admin.createTable(tableDescriptor);
			table.setOption(Table.TableOption.BUFFERWRITE, true);

			return table;
		} else {
			return MapRDB.getTable(tablePath);
		}
	}

	// Add histogram to document with id "documentkey.timestamp"
	public void addHistogram(String documentKey,long timestamp, String tags, FloatHistogram histogram, String metricName, double count, int windowDuration) throws Exception{
		String query = documentKey+timestamp;
		log.info("Adding document: "+query);
		byte[] tempByteArray = SerializationUtils.serialize((Serializable) histogram);
		Document rec = Json.newDocument()
				.set("histogram",Base64.encodeBase64String(tempByteArray))
				.set("timestamp",timestamp)
				.set("hash",StringsUtil.getHashForTags(tags))
				.set("tags",tags)
				.set("metricname", metricName)
				.set("windowduration", windowDuration)
				.set("count", count);

		table.insert(query, rec);
	}

	// Add histogram to document with id "metricName.timestamp"
	public void addTDigest(String metricName,long timestamp, String tags, TDigest tDigest, double count, int windowDuration) throws Exception{
		String hash = StringsUtil.getHashForTags(tags);
		Calendar now = Calendar.getInstance();
		//get current TimeZone using getTimeZone method of Calendar class
    TimeZone timeZone = now.getTimeZone();
    Calendar previous = DateTime.previousInterval(System.currentTimeMillis(), 1, Calendar.HOUR_OF_DAY, timeZone);
    String query = metricName+previous.getTimeInMillis()+hash;
		log.info("Adding document: "+query);
		byte[] tempByteArray = SerializationUtils.serialize((Serializable) tDigest);
		Document rec = Json.newDocument()
				.set("tdigest",Base64.encodeBase64String(tempByteArray))
				.set("timestamp",timestamp)
				.set("hash",hash)
				.set("tags",tags)
				.set("windowduration", windowDuration)
				.set("count", count)
				.set("hour", now.get(Calendar.HOUR_OF_DAY))
				.set("minute", now.get(Calendar.MINUTE));
		table.insert(query, rec);
		now = null;
	}

	// TODO - Make this function less generic to separate data loading vs testing scenarios
	// Add comments
	public boolean updateAggregates(Metric metric, int threshold, long duration, boolean disableAlerts) throws Exception {
		long firstRaised = 0, lastUpdated = 0;
		boolean raiseAlarm = false;
		String tDigest="";
		Date date = new Date(metric.getTimeStamp()*1000);
		Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
		calendar.setTime(date);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

		HashMap<String,String> tagsMap = (HashMap<String, String>) metric.getTags();
		StringBuffer tags= new StringBuffer();
		for (Entry<String, String> e: tagsMap.entrySet()) {
			if (!e.getKey().equalsIgnoreCase("value")) {
				tags.append(e.getKey()+"="+e.getValue()+",");
			}
		}
		String query = MetricsDao.days[dayOfWeek-1]+"."+hour+"."+tags.substring(0, tags.length()-1)+".";
		Document document = table.findById("metrics"+"."+metric.getMetricName());
		if (document != null) {
			try {
				tDigest = document.getString(query+"tdigest");
			} catch (NoSuchElementException ne) {
				tDigest = "";
			} catch (Exception e) {
				log.error("Failed with exception: "+e.getStackTrace());
				return false;
			}
		}

		TDigest td;
		if (tDigest == null || tDigest.isEmpty()) {
			System.out.println("Creating a new tdigest for query: "+query);
			td = new MergingDigest(200);
		} else {
			td = SerializationUtils.deserialize(Base64.decodeBase64(tDigest));
		}

		if (!disableAlerts) {
			System.out.println("Query: "+query);
			// Check for values and raise alarm if the metric value is above threshold
			Document alertsDocument = table.findById("alerts"+"."+metric.getMetricName());
			DocumentMutation alertsMutation=null;

			double p = td.cdf(metric.getValue());
			p = Math.log10(p / (1 - p));
			System.out.println("Metric value  , probability , odds  , timestamp");
			System.out.printf("%.6f, %.6f, %.6f, %s\n",metric.getValue(), td.cdf(metric.getValue()), p, new Date(metric.getTimeStamp()*1000));
			System.out.println();

			if (Math.abs(p) > threshold) {
				if (alertsDocument != null) {
					lastUpdated = metric.getTimeStamp();
					try {
						firstRaised = alertsDocument.getLong("firstRaised");
						if (lastUpdated - firstRaised >= duration && (firstRaised !=0 && lastUpdated !=0)) {
							System.out.println("Flag raised for metrics between timeStamps: "+new Date(firstRaised*1000)+" and "+new Date(metric.getTimeStamp()*1000));
							raiseAlarm = true;
							firstRaised = metric.getTimeStamp();
						} else {
							firstRaised = (firstRaised != 0) ? Math.min(metric.getTimeStamp(),firstRaised):metric.getTimeStamp();
						}
					} catch(NoSuchElementException ne) {
						firstRaised = 0;
					}
				} else {
					lastUpdated = firstRaised = metric.getTimeStamp();
				}
			}

			alertsMutation  = MapRDB.newMutation()
					.set("firstRaised",firstRaised)
					.set("lastUpdated",lastUpdated);
			table.update("alerts"+"."+metric.getMetricName(),alertsMutation);
		}

		td.add(metric.getValue());
		byte[] tempByteArray = SerializationUtils.serialize((Serializable) td);
		tDigest = Base64.encodeBase64String(tempByteArray);

		DocumentMutation mutation = MapRDB.newMutation()
				.set(query+"tdigest",tDigest)
				.set(query+"lastUpdated",System.currentTimeMillis());


		table.update("metrics"+"."+metric.getMetricName(), mutation);
		return raiseAlarm;

	}
}
