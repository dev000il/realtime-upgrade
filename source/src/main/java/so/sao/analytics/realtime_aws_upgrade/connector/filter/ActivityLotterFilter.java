package so.sao.analytics.realtime_aws_upgrade.connector.filter;

import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;

import so.sao.analytics.sdk.common.model.flatevent.FlatActivity;


/**
 * This class is a basic implementation of IFilter that returns true for all records.
 * 
 * @param <FlatActivity>
 */
public class ActivityLotterFilter implements IFilter<FlatActivity> {
    private static final long serialVersionUID = -42352706089818030L;

	@Override
	public boolean keepRecord(FlatActivity act) {
	    return act.getAt() == 2;
	}


}
