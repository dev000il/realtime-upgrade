package so.sao.analytics.realtime_aws_upgrade.connector.emitter;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

import so.sao.analytics.pipeline.common.utils.RedisKeyCmd;
import so.sao.analytics.pipeline.common.utils.RedisStringCmd;
import so.sao.analytics.realtime_aws_upgrade.common.Constants;
import so.sao.analytics.realtime_aws_upgrade.model.ActivityCount;
import so.sao.analytics.realtime_aws_upgrade.utils.ElasticCacheKinesisConnectorConfiguration;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

/**
 * The implementation of the iemitter is used to store the movement from the Amazon to Cache Elastic. 
 * Use this class to configure the installation of a redis cluster. 
 * The transmit method writes the statistics of the buffer to the Cache Elastic.
 * This kind of activityaggregator.
 */
public class SumActivityEmitter implements IEmitter<ActivityCount> {

    private static final long serialVersionUID = 1845717648422837414L;

    static final Logger logger = LoggerFactory.getLogger(SumActivityEmitter.class);

    public SumActivityEmitter(ElasticCacheKinesisConnectorConfiguration configuration) {
        logger.info("SumActivityEmitter.....");
    }

    
    @Override
    public List<ActivityCount> emit(final UnmodifiableBuffer<ActivityCount> buffer) {
    	List<ActivityCount> records = buffer.getRecords();
    	try {
			complete(records);
		} catch (IOException e) {
			logger.info("SumActivityEmitter Error.....");
		}
    	return Collections.emptyList();
    }

    @Override
    public void fail(List<ActivityCount> records) {

        for (ActivityCount record : records) {
        	logger.error("Record failed: " + record);
        }
    }

    @Override
    public void shutdown() {
    	logger.error("Record shutting down: " );
    }

    public void complete(List<ActivityCount> records) throws IOException {

        long t1 = System.currentTimeMillis();

        if (records.isEmpty()) {
            return;
        }

        Map<String, String> scanMap = new HashMap<>();
        Map<String, Set<String>> memberMap = new HashMap<>();
        Map<String, String> lotteryMap = new HashMap<>();
        Map<String, String> rewardAmtMap = new HashMap<>();

        try {
            for (ActivityCount ac : records) {
                String key = Integer.toString(ac.getCompanyId());
                switch (ac.getActivityType()) {
                case 1:
                    if (scanMap.containsKey(key)) {
                        int scanTmpCnt = Integer.parseInt(scanMap.get(key)) + 1;
                        scanMap.put(key, Integer.toString(scanTmpCnt));

                    } else {
                        scanMap.put(key, Integer.toString(1));
                    }
                    break;
                case 2:
                    if (lotteryMap.containsKey(key)) {
                        int lotteryTmpCnt = Integer.parseInt(lotteryMap.get(key)) + 1;
                        lotteryMap.put(key, Integer.toString(lotteryTmpCnt));
                    } else {
                        lotteryMap.put(key, Integer.toString(1));
                    }

                    if (memberMap.containsKey(key)) {
                        Set<String> memberTmp = memberMap.get(key);
                        memberTmp.add(ac.getUserId());
                        memberMap.put(key, memberTmp);
                    } else {
                        Set<String> memberTmp = new HashSet<>();
                        memberTmp.add(ac.getUserId());
                        memberMap.put(key, memberTmp);
                    }

                    if (rewardAmtMap.containsKey(key)) {
                        if (ac.getRewardAmount() != null && !ac.getRewardAmount().isEmpty()
                                && !ac.getRewardAmount().equals("0")) {
                            BigDecimal rewardAmtTmp = new BigDecimal(rewardAmtMap.get(key));
                            BigDecimal rewardAmtCnt = rewardAmtTmp.add(new BigDecimal(ac.getRewardAmount()));
                            rewardAmtMap.put(key, rewardAmtCnt.toString());
                        }
                    } else {
                        if (ac.getRewardAmount() != null && !ac.getRewardAmount().isEmpty()
                                && !ac.getRewardAmount().equals("0")) {
                            rewardAmtMap.put(key, ac.getRewardAmount());
                        }
                    }
                    break;
                default:
                    break;
                }
            }

            increaseActivityCnt(Constants.scanActivityKey, scanMap);
            increaseActivityCnt(Constants.enterLotteryKey, lotteryMap);
            increaseActivityMembers(memberMap);
            increaseRewardAmount(rewardAmtMap);
            
        } catch (Exception e) {
            logger.error("Unknow exception.", e);
            throw new IOException(e);
        }
        long t2 = System.currentTimeMillis();

        logger.info("RealTime sum activity count queue[{}], size: [{}], spend: [{}]", records.size(), (t2 - t1));
    }

    public void increaseRewardAmount(Map<String, String> rewardAmounts) throws IOException {
        String cacheKey = Constants.rewardAmountKey;
        if (rewardAmounts.isEmpty()) {
            // logger.info("Activity type [{}] is empty.", cacheKey);
            return;
        }

        try {
            String cacheRewardAmt = RedisStringCmd.get(cacheKey);
            // logger.info("Cache reward amount count: {}", cacheRewardAmt);

            if (cacheRewardAmt == null || cacheRewardAmt.isEmpty() || "{}".equals(cacheRewardAmt)) {
                RedisStringCmd.set(cacheKey, JSON.toJSONString(rewardAmounts));
            } else {
                Map<String, String> cacheRewardAmts = JSON.parseObject(cacheRewardAmt, Map.class);
                for (String key : rewardAmounts.keySet()) {
                    BigDecimal newAmount = new BigDecimal(rewardAmounts.get(key));
                    if (cacheRewardAmts.containsKey(key)) {
                        BigDecimal cacheAmount = new BigDecimal(cacheRewardAmts.get(key));
                        BigDecimal rewardAmt = newAmount.add(cacheAmount);
                        cacheRewardAmts.put(key, rewardAmt.toString());
                    } else {
                        cacheRewardAmts.put(key, newAmount.toString());
                    }
                }
                RedisStringCmd.set(cacheKey, JSON.toJSONString(cacheRewardAmts));
                logger.info("Reward amounts: {}", cacheRewardAmts);
            }

            RedisKeyCmd.expire(cacheKey, getExpireTime());
        } catch (Exception e) {
            logger.error("Exception when increase lottery reward amount", e);
            throw new IOException(e);
        }
    }

    public void increaseActivityMembers(Map<String, Set<String>> membersCnt) throws IOException {
        String cacheKey = Constants.membersActivityKey;
        if (membersCnt.isEmpty()) {
            // logger.info("Activity type [{}] is empty.", cacheKey);
            return;
        }

        try {
            String cacheMember = RedisStringCmd.get(cacheKey);
            // logger.info("Cache members count: {}", cacheMember);

            if (cacheMember == null || cacheMember.isEmpty() || "{}".equals(cacheMember)) {
                RedisStringCmd.set(cacheKey, JSON.toJSONString(membersCnt));
            } else {
                Map<String, Set<String>> cacheMembers = new HashMap<>();
                // translate json array to hash set
                Map<String, JSONArray> cacheTemp = JSON.parseObject(cacheMember, Map.class);
                for (String key : cacheTemp.keySet()) {
                    Set<String> temp = new HashSet<>(JSON.parseArray(JSON.toJSONString(cacheTemp.get(key)), String.class));
                    if (membersCnt.containsKey(key)) {
                        temp.addAll(membersCnt.get(key));
                        cacheMembers.put(key, temp);
                        membersCnt.remove(key);
                    } else {
                        cacheMembers.put(key, temp);
                    }
                }

                if (!membersCnt.isEmpty()) {
                    for (String key : membersCnt.keySet()) {
                        cacheMembers.put(key, membersCnt.get(key));
                    }
                }

                RedisStringCmd.set(cacheKey, JSON.toJSONString(cacheMembers));
                
                StringBuffer temp = new StringBuffer(",");
                for(String company : cacheMembers.keySet()){
                    temp.append(company).append("=").append(cacheMembers.get(company).size()).append(", ");
                }
                temp.deleteCharAt(temp.length()-1);
                temp.deleteCharAt(temp.length()-2);
                temp.append("}");
                logger.info("Members count: {}", temp);
            }

            RedisKeyCmd.expire(cacheKey, getExpireTime());
        } catch (Exception e) {
            logger.error("Exception when increase member activity count", e);
            throw new IOException(e);
        }
    }

    public void increaseActivityCnt(String cacheKey, Map<String, String> activityCnts) throws IOException {
        // filter empty data
        if (activityCnts.isEmpty()) {
            // logger.info("Activity type [{}] is empty.", cacheKey);
            return;
        }

        try {
            String cacheActCnt = RedisStringCmd.get(cacheKey);
            // logger.info("Cache activity count: {}", cacheActCnt);

            if (cacheActCnt == null) {
                RedisStringCmd.set(cacheKey, JSON.toJSONString(activityCnts));
            } else {
                Map<String, String> cacheActivityCnts = JSON.parseObject(cacheActCnt, Map.class);
                for (String key : activityCnts.keySet()) {
                    int newCnt = Integer.parseInt(activityCnts.get(key));
                    if (cacheActivityCnts.containsKey(key)) {
                        int cacheCnt = Integer.parseInt(cacheActivityCnts.get(key));
                        cacheActivityCnts.put(key, Integer.toString(newCnt + cacheCnt));
                    } else {
                        cacheActivityCnts.put(key, Integer.toString(newCnt));
                    }
                }
                RedisStringCmd.set(cacheKey, JSON.toJSONString(cacheActivityCnts));
                logger.info("Activity [{}] count: {}", cacheKey, cacheActivityCnts);
            }

            RedisKeyCmd.expire(cacheKey, getExpireTime());
        } catch (Exception e) {
            logger.error("Exception when increase activity count", e);
            throw new IOException(e);
        }
    }

    public int getExpireTime() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        long curTime = cal.getTimeInMillis();

        cal.add(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        long nextTime = cal.getTimeInMillis();
        int expireTime = Long.valueOf((nextTime - curTime) / 1000).intValue();
        // logger.info("Current TTL: {}", expireTime);
        return expireTime;
    }
    
    
    
    
    
    
    
    
    
    

}
