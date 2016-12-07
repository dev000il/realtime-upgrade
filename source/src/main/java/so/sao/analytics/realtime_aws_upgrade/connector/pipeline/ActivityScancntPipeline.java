package so.sao.analytics.realtime_aws_upgrade.connector.pipeline;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;

import so.sao.analytics.realtime_aws_upgrade.connector.emitter.SumActivityEmitter;
import so.sao.analytics.realtime_aws_upgrade.connector.filter.ActivityScanFilter;
import so.sao.analytics.realtime_aws_upgrade.connector.transformer.FlatSumActivityTransformer;
import so.sao.analytics.realtime_aws_upgrade.model.ActivityCount;
import so.sao.analytics.realtime_aws_upgrade.utils.ElasticCacheKinesisConnectorConfiguration;
import so.sao.analytics.sdk.common.model.flatevent.FlatActivity;

/**
 * The Pipeline. Processes FlatActivity records in Kinesis
 * format. Uses:
 * <ul>
 * <li>SumActivityEmitter</li>
 * <li>BasicMemoryBuffer</li>
 * <li>FlatSumActivityTransformer</li>
 * <li>ActivityScanFilter</li>
 * </ul>
 */
public class ActivityScancntPipeline implements IKinesisConnectorPipeline<FlatActivity, ActivityCount> {

    @Override
    public IEmitter<ActivityCount> getEmitter(KinesisConnectorConfiguration configuration) {
        return new SumActivityEmitter((ElasticCacheKinesisConnectorConfiguration) configuration);
    }

    @Override
    public IBuffer<FlatActivity> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<FlatActivity>(configuration);
    }

    @Override
    public ITransformer<FlatActivity, ActivityCount> getTransformer(KinesisConnectorConfiguration configuration) {
    	return new FlatSumActivityTransformer();
    }

    @Override
    public IFilter<FlatActivity> getFilter(KinesisConnectorConfiguration configuration) {
        return new ActivityScanFilter();
    }

}
