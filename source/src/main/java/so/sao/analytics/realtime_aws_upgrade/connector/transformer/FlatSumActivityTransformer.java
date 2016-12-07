package so.sao.analytics.realtime_aws_upgrade.connector.transformer;

import java.math.BigDecimal;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;

import so.sao.analytics.realtime_aws_upgrade.model.ActivityCount;
import so.sao.analytics.sdk.common.model.flatevent.FlatActivity;
import so.sao.analytics.sdk.kinesis.events.ActivityTransformer;



/**
 * A custom transformer for {@link FlatActivity} records in Kinesis. 
 * The output is key/value pairs which are compatible with HBase insertions
 */
public class FlatSumActivityTransformer extends ActivityTransformer<ActivityCount>
implements ITransformer<FlatActivity, ActivityCount> {

	/**
	 * from FlatActivity to ActivityCount like FlatSumActivityTransformer
	 * @param FlatActivity
	 * return ActivityCount
	 */
    @Override
    public ActivityCount fromClass(FlatActivity act) {
        ActivityCount ac = null;
        short at = Short.valueOf(String.valueOf(act.getAt()));
        if (act.getRwds() == null || act.getRwds().isEmpty()) {
            ac = new ActivityCount(at, act.getC(), act.getU(), null);
        } else {
            BigDecimal amount = null;
            for (String key : act.getRwds().keySet()) {
                BigDecimal tmp = new BigDecimal(act.getRwds().get(key));
                if (amount != null) {
                    amount = tmp.add(amount);
                } else {
                    amount = tmp;
                }
            }
            ac = new ActivityCount(at, act.getC(), act.getU(), amount.toString());
        }
        return ac;
    }
}
