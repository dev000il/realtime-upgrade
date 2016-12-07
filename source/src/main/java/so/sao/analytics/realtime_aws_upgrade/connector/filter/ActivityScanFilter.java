package so.sao.analytics.realtime_aws_upgrade.connector.filter;

import so.sao.analytics.sdk.common.model.flatevent.FlatActivity;

import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;

/**
 * This class is a basic implementation of IFilter that returns true for all records.
 * 
 * @param <FlatActivity>
 */
public class ActivityScanFilter implements IFilter<FlatActivity> {
    private static final long serialVersionUID = -42352706089818030L;
	
    @Override
    public boolean keepRecord(FlatActivity act) {
    	return act.getAt() == 1;
    }

}
