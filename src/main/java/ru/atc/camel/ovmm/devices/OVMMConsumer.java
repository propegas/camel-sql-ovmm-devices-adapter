package ru.atc.camel.ovmm.devices;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.cache.CacheConstants;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.camel.model.ModelCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.at_consulting.itsm.device.Device;

//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCDevice;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCAlarmSeverity;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCAlarm;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCDevice;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;

//import ru.at_consulting.itsm.device.Device;
import ru.at_consulting.itsm.event.Event;
//import ru.atc.camel.nnm.devices.WsdlNNMConsumer.PersistentEventSeverity;
import ru.atc.camel.ovmm.devices.api.OVMMDevices;
import ru.atc.camel.ovmm.devices.api.OVMMEvents;

import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

//import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Driver;



public class OVMMConsumer extends ScheduledPollConsumer {
	
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	
	public static OVMMEndpoint endpoint;
	
	public String vm_ping_col =  "attribute043";
	public String vm_snmp_col =  "attribute044";
	public String vm_vmtools_col =  "attribute045";
	public String vm_backup_col =  "attribute046";
	public String vm_power_col =  "attribute004";
	
	public static ModelCamelContext context;
	
	public enum PersistentEventSeverity {
	    OK, INFO, WARNING, MINOR, MAJOR, CRITICAL;
		
	    public String value() {
	        return name();
	    }

	    public static PersistentEventSeverity fromValue(String v) {
	        return valueOf(v);
	    }
	}

	public OVMMConsumer(OVMMEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        OVMMConsumer.endpoint = endpoint;
        //this.afterPoll();
        this.setTimeUnit(TimeUnit.MINUTES);
        this.setInitialDelay(0);
        this.setDelay(endpoint.getConfiguration().getDelay());
	}
	
	public static ModelCamelContext getContext() {
		// TODO Auto-generated method stub
				return context;
	}
	
	public static void setContext(ModelCamelContext context1){
		context = context1;

	}

	@Override
	protected int poll() throws Exception {
		
		String operationPath = endpoint.getOperationPath();
		
		if (operationPath.equals("devices")) return processSearchDevices();
		
		// only one operation implemented for now !
		throw new IllegalArgumentException("Incorrect operation: " + operationPath);
	}
	
	@Override
	public long beforePoll(long timeout) throws Exception {
		
		logger.info("*** Before Poll!!!");
		// only one operation implemented for now !
		//throw new IllegalArgumentException("Incorrect operation: ");
		
		//send HEARTBEAT
		genHeartbeatMessage(getEndpoint().createExchange());
		
		return timeout;
	}
	
	public static void genHeartbeatMessage(Exchange exchange) {
		// TODO Auto-generated method stub
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp / 1000;
		//String textError = "Возникла ошибка при работе адаптера: ";
		Event genevent = new Event();
		genevent.setMessage("Сигнал HEARTBEAT от адаптера");
		genevent.setEventCategory("ADAPTER");
		genevent.setObject("HEARTBEAT");
		genevent.setSeverity(PersistentEventSeverity.OK.name());
		genevent.setTimestamp(timestamp);
		genevent.setEventsource("OVMM_DEVICE_ADAPTER");
		
		logger.info(" **** Create Exchange for Heartbeat Message container");
        //Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);
        
        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Heartbeats");
        exchange.getIn().setHeader("Type", "Heartbeats");
        exchange.getIn().setHeader("Source", "OVMM_DEVICE_ADAPTER");

        try {
        	//Processor processor = getProcessor();
        	//.process(exchange);
        	//processor.process(exchange);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} 
	}
	
	private void genErrorMessage(String message) {
		// TODO Auto-generated method stub
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp / 1000;
		String textError = "Возникла ошибка при работе адаптера: ";
		Event genevent = new Event();
		genevent.setMessage(textError + message);
		genevent.setEventCategory("ADAPTER");
		genevent.setSeverity(PersistentEventSeverity.CRITICAL.name());
		genevent.setTimestamp(timestamp);
		genevent.setEventsource("OVMM_DEVICE_ADAPTER");
		genevent.setStatus("OPEN");
		genevent.setHost("adapter");
		
		logger.info(" **** Create Exchange for Error Message container");
        Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);
        
        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Events");

        try {
			getProcessor().process(exchange);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	// "throws Exception" 
	private int processSearchDevices()  throws Exception, Error, SQLException {
		
		//Long timestamp;
		DataSource dataSource = setupDataSource();
		
		List<HashMap<String, Object>> listAllDevices = new ArrayList<HashMap<String,Object>>();
		int devicesCount = 0;
		//int statuses = 0;
		try {
			
			logger.info( String.format("***Try to get All Devices ***"));
			
			listAllDevices = getAllDevices(dataSource);
			
			logger.info( String.format("***Received %d Devices from SQL***", listAllDevices.size()));
			String title, type, id;
			
			logger.info( String.format("***Try to generate Device Objects***"));
			for(int i=0; i < listAllDevices.size(); i++) {
			  	
				HashMap<String, Object> device = listAllDevices.get(i);
				id = device.get("id").toString();
				title = device.get("title").toString();
				type  = device.get("type").toString();
				
				logger.debug("MYSQL row " + i + ": " + id + 
						" " + title + 
						" " + type);
				
				Device gendevice = genDeviceObj(device);
				
				logger.debug("*** Create CI Exchange ***" );
				
				Exchange exchange = getEndpoint().createExchange();
		        exchange.getIn().setBody(gendevice, Device.class);
		        exchange.getIn().setHeader("DeviceId", gendevice.getId());
		        exchange.getIn().setHeader("DeviceType", gendevice.getDeviceType());
		        exchange.getIn().setHeader("ParentId", gendevice.getParentID());
		        exchange.getIn().setHeader("queueName", "Devices");
				//exchange.getIn().setHeader("DeviceType", vmevents.get(i).getDeviceType());
				
				

				try {
					getProcessor().process(exchange);
					devicesCount++;
					
					//File cachefile = new File("sendedEvents.dat");
					//removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error( String.format("Error while process Exchange message: %s ", e));
				} 
						
						
			}
			
			logger.info( String.format("***Received %d Devices from SQL*** ", listAllDevices.size()));
	  
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
			return 0;
		}
		catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
			return 0;
		}
		catch (Error e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
			return 0;
		}
		catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			return 0;
		}
		finally
		{
			//return 0;
		}
		
		
		logger.info( String.format("***Sended to Exchange messages: %d ***", devicesCount));
	
        return 1;
	}
	
	private Device genDeviceObj( HashMap<String, Object> device  ) {
		Device gendevice = new Device();
		
		String vm_type_name= endpoint.getConfiguration().getVm_type();
		String deviceType = "";
		String ttt = "ttt";
		
		logger.debug("*** endpoint.getConfiguration().getVmDeviceType(): " + endpoint.getConfiguration().getVmDeviceType());
		logger.debug("*** endpoint.getConfiguration().getVm_type(): " + endpoint.getConfiguration().getVm_type());
		logger.debug("*** device.get(type).toString(): " + device.get("type").toString());
		logger.debug("*** type: " + device.get("type").toString());
		logger.debug("*** 123: " + ttt);
		logger.debug("*** vm_type_name: " + vm_type_name);
		
		if (device.get("type").toString().equals(vm_type_name)){
			deviceType = endpoint.getConfiguration().getVmDeviceType();
		}
		else {
			deviceType = device.get("type").toString();
		}
		
		//String hostName = "";
		//hostName = node.getName();
		gendevice.setName(device.get("title").toString());
		//gendevice.setHostName(node.getLongName());
		//gendevice.setSystemName(node.getSystemName());
		gendevice.setDeviceType(deviceType);
		//gendevice.setModelName(node.getDeviceModel());
		//gendevice.setDeviceState(setRightStatus(node.getStatus().name()));
		gendevice.setId("OVMM:" + device.get("id").toString());
		//gendevice.setParentID(node.getCustomAttributes()[0].getValue());
		//gendevice.set(node.getUuid());
		//gendevice.setGroups(groupNames);
		gendevice.setSource("OVMM");
		gendevice.setParentID("OVMM:" + device.get("parent_id").toString());
		//gendevice.set
		
		logger.debug(gendevice.toString());
		
		return gendevice;
				
	}
	
	private List<HashMap<String, Object>> getAllDevices(DataSource dataSource) throws SQLException {
		// TODO Auto-generated method stub
        
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
        Connection con = null; 
        PreparedStatement pstmt;
        ResultSet resultset = null;
        try {
        	con = (Connection) dataSource.getConnection();
			//con.setAutoCommit(false);
			
            pstmt = con.prepareStatement("SELECT tree.id, tree.title, tree.type, tree.parent_id " +
                        "FROM tree " +
                        "WHERE tree.type != ?");
                       // +" LIMIT ?;");
            pstmt.setString(1, "null");
            //pstmt.setInt(2, 3);
            
            logger.debug("MYSQL query: " +  pstmt.toString()); 
            resultset = pstmt.executeQuery();
            //con.commit();
            
            list = convertRStoList(resultset);
            
            
            resultset.close();
            pstmt.close();
            
            if (con != null) con.close();
            
            return list;
            
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logger.error( String.format("Error while SQL execution: %s ", e));
			
			if (con != null) con.close();
			
			throw e;

		} finally {
            if (con != null) con.close();
            
            //return list;
        }
		
	}
	
	private List<HashMap<String, Object>> convertRStoList(ResultSet resultset) throws SQLException {
		
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
		try {
			ResultSetMetaData md = resultset.getMetaData();
	        int columns = md.getColumnCount();
	        //result.getArray(columnIndex)
	        //resultset.get
	        logger.debug("MYSQL columns count: " + columns); 
	        
	        resultset.last();
	        int count = resultset.getRow();
	        logger.debug("MYSQL rows2 count: " + count); 
	        resultset.beforeFirst();
	        
	        int i = 0, n = 0;
	        //ArrayList<String> arrayList = new ArrayList<String>(); 
	
	        while (resultset.next()) {              
	        	HashMap<String,Object> row = new HashMap<String, Object>(columns);
	            for(int i1=1; i1<=columns; ++i1) {
	                row.put(md.getColumnLabel(i1),resultset.getObject(i1));
	            }
	            list.add(row);                 
	        }
	        
	        return list;
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			return null;

		} finally {

		}
	}

	private DataSource setupDataSource() {
		
		String url = String.format("jdbc:mysql://%s:%s/%s",
		endpoint.getConfiguration().getMysql_host(), endpoint.getConfiguration().getMysql_port(),
		endpoint.getConfiguration().getMysql_db());
		
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername( endpoint.getConfiguration().getUsername() );
        ds.setPassword( endpoint.getConfiguration().getPassword() );
        ds.setUrl(url);
              
        return ds;
    }
	
	public static String setRightValue(String colour)
	{
		String newstatus = "";
		
		switch (colour) {
        	case "#006600":  newstatus = "OK";break;
        	case "#FF0000":  newstatus = "ERROR";break;
        	default: newstatus = "NA";break;

        	
		}
		/*
		System.out.println("***************** colour: " + colour);
		System.out.println("***************** status: " + newstatus);
		*/
		return newstatus;
	}
	
	public static String setRightSeverity(String colour)
	{
		String newseverity = "";
			
		switch (colour) {
        	case "#006600":  newseverity = PersistentEventSeverity.OK.name();break;
        	case "#FF0000":  newseverity = PersistentEventSeverity.CRITICAL.name();break;
        	default: newseverity = PersistentEventSeverity.INFO.name();break;

        	
		}
		/*
		System.out.println("***************** colour: " + colour);
		System.out.println("***************** newseverity: " + newseverity);
		*/
		return newseverity;
	}
	
	public void removeLineFromFile(String file, String lineToRemove) {
		BufferedReader br = null;
		PrintWriter pw = null;
	    try {

	      File inFile = new File(file);

	      if (!inFile.isFile()) {
	        System.out.println("Parameter is not an existing file");
	        return;
	      }

	      //Construct the new file that will later be renamed to the original filename.
	      File tempFile = new File(inFile.getAbsolutePath() + ".tmp");

	      br = new BufferedReader(new FileReader(file));
	      pw = new PrintWriter(new FileWriter(tempFile));

	      String line = null;

	      //Read from the original file and write to the new
	      //unless content matches data to be removed.
	      while ((line = br.readLine()) != null) {

	        if (!line.trim().equals(lineToRemove)) {

	          pw.println(line);
	          pw.flush();
	        }
	      }
	      pw.close();
	      br.close();

	      //Delete the original file
	      if (!inFile.delete()) {
	        System.out.println("Could not delete file");
	        return;
	      }

	      //Rename the new file to the filename the original file had.
	      if (!tempFile.renameTo(inFile))
	        System.out.println("Could not rename file");

	    }
	    catch (FileNotFoundException ex) {
	      ex.printStackTrace();
	    }
	    catch (IOException ex) {
	      ex.printStackTrace();
	    }
	    finally {
	    	try {
	    		pw.close();
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	  }

}