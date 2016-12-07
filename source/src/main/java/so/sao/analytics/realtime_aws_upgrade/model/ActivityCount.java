package so.sao.analytics.realtime_aws_upgrade.model;

public class ActivityCount {

    private Short activityType;

    private Integer companyId;

    private String userId;

    private String rewardAmount;

    public ActivityCount(Short activityType, Integer companyId, String userId, String rewardAmount) {
        this.activityType = activityType;
        this.companyId = companyId;
        this.userId = userId;
        this.rewardAmount = rewardAmount;
    }

    public Short getActivityType() {
        return activityType;
    }

    public void setActivityType(Short activityType) {
        this.activityType = activityType;
    }

    public Integer getCompanyId() {
        return companyId;
    }

    public void setCompanyId(Integer companyId) {
        this.companyId = companyId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRewardAmount() {
        return rewardAmount;
    }

    public void setRewardAmount(String rewardAmount) {
        this.rewardAmount = rewardAmount;
    }

    @Override
    public String toString() {
        return "ActivityCount{" + "activityType=" + activityType + ", companyId=" + companyId + ", userId='" + userId + '\''
                + '}';
    }
}
