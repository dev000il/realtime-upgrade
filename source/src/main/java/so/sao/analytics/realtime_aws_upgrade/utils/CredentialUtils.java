package so.sao.analytics.realtime_aws_upgrade.utils;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;

/**
 * Provides utilities for retrieving credentials to talk to AWS
 */
public class CredentialUtils{
	
	public static AWSCredentialsProvider getCredentialsProvider(String awspath) throws Exception {
        /*
         * The SystemPropertiesCredentialsProvider will return your credential profile by
         * reading from the property
         */
        AWSCredentialsProvider credentialsProvider = null;
        
        try {
        	credentialsProvider = new ClasspathPropertiesFileCredentialsProvider(awspath);
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the Properties credential profiles file. " +
                    "Please make sure that your Properties credentials file is at the correct " +
                    "properties, and is in valid format.",
                    e);
        }
        return credentialsProvider;
    }
}
