package so.sao.analytics.realtime_aws_upgrade;

import so.sao.analytics.sdk.common.model.flatevent.FlatActivity;
import so.sao.analytics.realtime_aws_upgrade.connector.ActivityScancntExecutor;
import so.sao.analytics.realtime_aws_upgrade.kinesis.KinesisConnectorExecutor;
import so.sao.analytics.realtime_aws_upgrade.model.ActivityCount;
import so.sao.analytics.realtime_aws_upgrade.connector.ActivityLotterycntExecutor;

/**
 * Executor issued a record to write to ElasticCache. 
 */
public class AllWorkePortal {
    private static final String CONFIG_FILE = "config.properties";
    /**
     * Main method to run the KinesisConnectorExecutor.
     * 
     * @param args
     */
    public static void main(String[] args) {
    	
    	//Creates a new activityHBaseExecutor Thread
        Thread activityScancntThread=new Thread(){ 
            public void run(){ 
                KinesisConnectorExecutor<FlatActivity, ActivityCount> activityScancntExecutor = new ActivityScancntExecutor(CONFIG_FILE);
                activityScancntExecutor.run();
            }
        };
        //Creates a new thirdpartyHBaseExecutor Thread
        Thread activityLotterycntThread=new Thread(){ 
            public void run(){ 
                KinesisConnectorExecutor<FlatActivity, ActivityCount> activityLotterycntExecutor = new ActivityLotterycntExecutor(CONFIG_FILE);
                activityLotterycntExecutor.run();
            }
        };

        //Thread start
        activityScancntThread.start();
        activityLotterycntThread.start();
    }
}
