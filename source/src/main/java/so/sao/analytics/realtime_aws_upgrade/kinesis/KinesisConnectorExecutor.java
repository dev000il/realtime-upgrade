package so.sao.analytics.realtime_aws_upgrade.kinesis;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import so.sao.analytics.realtime_aws_upgrade.utils.CredentialUtils;
import so.sao.analytics.realtime_aws_upgrade.utils.ElasticCacheKinesisConnectorConfiguration;

/**
 * This class defines the execution of a Amazon Kinesis Connector.
 * 
 */
public abstract class KinesisConnectorExecutor<T, U> extends KinesisConnectorExecutorBase<T, U> {
	
	private static final Logger logger = Logger.getLogger(KinesisConnectorExecutor.class);
	// Class variables
	protected final ElasticCacheKinesisConnectorConfiguration config;
	private final Properties properties;
	// properties kinesisInputStream name
	private static final String kinesisInputStream = "kinesisInputStream";
	// properties awsPath name
	private static final String awsPath = "aws.path";
	// properties appName name
	private static final String appName = "appName";
	/**
	 * Create a new KinesisConnectorExecutor based on the provided configuration (*.propertes) file.
	 * 
	 * @param configFile
	 *        The name of the configuration file to look for on the classpath
	 * @param streamName
	 * @param app
	 *        
	 */
	public KinesisConnectorExecutor(String configFile,String streamName,String app) {
		InputStream configStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);

		if (configStream == null) {
			String msg = "Could not find resource " + configFile + " in the classpath";
			throw new IllegalStateException(msg);
		}
		properties = new Properties();
		try {
			properties.load(configStream);
			configStream.close();
		} catch (IOException e) {
			String msg = "Could not load properties file " + configFile + " from classpath";
			throw new IllegalStateException(msg, e);
		}
		properties.setProperty(kinesisInputStream, properties.getProperty(streamName));
		properties.setProperty(appName, properties.getProperty(app));
		
		this.config = new ElasticCacheKinesisConnectorConfiguration(properties, getAWSCredentials(properties.getProperty(awsPath)));

		// Initialize executor with configurations
		super.initialize(config);
	}
	
	/**
	 * Returns an {@link AWSCredentialsProvider} with the permissions necessary to accomplish all specified
	 * tasks. At the minimum it will require read permissions for Amazon Kinesis. Additional read permissions
	 * and write permissions may be required based on the Pipeline used.
	 * 
	 * @return
	 */
	public AWSCredentialsProvider getAWSCredentials(String awsPath) {
		
		AWSCredentialsProvider credentialsProvider;
		try {
			credentialsProvider =  CredentialUtils.getCredentialsProvider(awsPath);
			return credentialsProvider;
		} catch (Exception e) {
			logger.error("Exception",e);
		};
		return null;
		
	}
}
