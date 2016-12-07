package so.sao.analytics.realtime_aws_upgrade.utils;

import java.util.Properties;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentialsProvider;

public class ElasticCacheKinesisConnectorConfiguration extends KinesisConnectorConfiguration {
	private static final Logger logger = Logger.getLogger(ElasticCacheKinesisConnectorConfiguration.class);

    
	/**
	 * Set KinesisConnector and EMR HBase Configuration from properties and AWSCredentialsProvider.
	 * 
	 * @param properties
	 * @param credentialsProvider
	 */
    public ElasticCacheKinesisConnectorConfiguration (Properties properties, AWSCredentialsProvider credentialsProvider) {
    	super(properties, credentialsProvider);
    }
    
    private int getIntegerProperty(String property, int defaultValue, Properties properties) {
        String propertyValue = properties.getProperty(property, Integer.toString(defaultValue));
        try {
            return Integer.parseInt(propertyValue.trim());
        } catch (NumberFormatException e) {
        	logger.error("NumberFormatException", e);
            return defaultValue;
        }
    }
}
