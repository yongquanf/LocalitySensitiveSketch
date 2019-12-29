package edu.harvard.syrah.sbon.async;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import edu.harvard.syrah.prp.Log;

public class Config {
	private static final Log log = new Log(Config.class);
	
	private static Properties configProps = System.getProperties();

	public static Properties getConfigProps() {
		return configProps;
	}

	public static void setConfigProps(Properties props) {
		configProps = props;
	}	
	
	public static String getProperty(String name, String defaultValue) {
		//log.debug("name=" + name);
		return configProps.getProperty(name, defaultValue);
	}
	
	public static String getProperty(String name) {
		return configProps.getProperty(name);
	}
	
	public static void read(String configRoot, String filename) {
		try {
			InputStream ip = new FileInputStream(filename);
			//log.info("config=" + filename);
			Properties configFileProps = new Properties();
			configFileProps.load(ip);

			for (Object propertyObj : configFileProps.keySet()) {
				String property = (String) propertyObj;
				String value = configFileProps.getProperty(property);
				if (!Config.getConfigProps().containsKey(property) && value != null && value.length() != 0) {
					//log.debug("prop=" + property + " val=" + value);
					Config.getConfigProps().setProperty(property, value);
				}
			}

			SortedMap<String, String> allProps = new TreeMap<String, String>();
			Properties cleanProps = new Properties(); 
			for (Object propertyObj : Config.getConfigProps().keySet()) {
			//for (Iterator<Object> objIt = Config.getConfigProps().keySet().iterator(); objIt.hasNext();) {
				String property = (String) propertyObj;
				if (property.startsWith(configRoot)) {
					String shortName = property.substring(configRoot.length() + 1, property.length());
					allProps.put(shortName, Config.getConfigProps().getProperty(property));
					cleanProps.put(shortName, Config.getConfigProps().getProperty(property));
				}
			}
			
			Config.configProps = cleanProps;
			
			log.info("configProperties=" + allProps.toString());
			//log.debug("cleanProps=" + cleanProps.toString());
		} catch (FileNotFoundException e1) {
			log.error("Could not open config file: " + e1);
		} catch (IOException e2) {
			log.error("Could not load config properties from file: " + e2);
		}
	}
	
}
