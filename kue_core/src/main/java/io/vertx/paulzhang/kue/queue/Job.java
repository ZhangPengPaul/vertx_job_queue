package io.vertx.paulzhang.kue.queue;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;

import java.util.Objects;
import java.util.UUID;

/**
 * Created by PaulZhang on 2016/9/13.
 */
@DataObject(generateConverter = true)
public class Job {

    private static Logger LOGGER = LoggerFactory.getLogger(Job.class);

    private static Vertx vertx;

    private static RedisClient redisClient;

    private static EventBus eventBus;

    // properties

    /**
     * UUID,event bus地址
     */
    private String addressId;
    /**
     * 任务编号
     */
    private long id = -1;
    /**
     * zset操作对应编号
     */
    private String zid;
    /**
     * 任务类型
     */
    private String type;
    /**
     * 任务数据
     */
    private JsonObject data;
    /**
     * 优先级
     */
    private Priority priority = Priority.NORMAL;
    /**
     * 任务状态
     */
    private JobState jobState = JobState.INACTIVE;
    /**
     * 延迟时间
     */
    private long delay = 0;
    /**
     * 最大重试次数
     */
    private int maxAttempts = 1;
    /**
     * 任务完成时是否自动删除
     */
    private boolean removeOnComplete = false;
    /**
     * TTL
     */
    private int ttl = 0;
    /**
     * 任务重试配置
     */
    private JsonObject backoff;

    /**
     * 任务已经重试的次数
     */
    private int attempts = 0;
    /**
     * 任务进度
     */
    private int progress = 0;
    /**
     * 任务执行结果
     */
    private JsonObject result;

    // metrics
    /**
     * 创建时间
     */
    private long createAt;
    /**
     * 从延时到等待的时间
     */
    private long promoteAt;
    /**
     * 更新时间
     */
    private long updatedAt;
    /**
     * 失败的时间
     */
    private long failedAt;
    /**
     * 任务开始时间
     */
    private long startedAt;
    /**
     * 任务处理花费时长
     */
    private long duration;

    public void setId(long id) {
        this.id = id;
    }

    public String getZid() {
        return zid;
    }

    public void setZid(String zid) {
        this.zid = zid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public JsonObject getData() {
        return data;
    }

    public void setData(JsonObject data) {
        this.data = data;
    }

    public Priority getPriority() {
        return priority;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    public JobState getJobState() {
        return jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public boolean isRemoveOnComplete() {
        return removeOnComplete;
    }

    public void setRemoveOnComplete(boolean removeOnComplete) {
        this.removeOnComplete = removeOnComplete;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public JsonObject getBackoff() {
        return backoff;
    }

    public void setBackoff(JsonObject backoff) {
        this.backoff = backoff;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public JsonObject getResult() {
        return result;
    }

    public void setResult(JsonObject result) {
        this.result = result;
    }

    public long getCreateAt() {
        return createAt;
    }

    public void setCreateAt(long createAt) {
        this.createAt = createAt;
    }

    public long getPromoteAt() {
        return promoteAt;
    }

    public void setPromoteAt(long promoteAt) {
        this.promoteAt = promoteAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public long getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(long failedAt) {
        this.failedAt = failedAt;
    }

    public long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(long startedAt) {
        this.startedAt = startedAt;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public Job() {
        this.addressId = UUID.randomUUID().toString();
        checkStaticMember();
    }

    public Job(JsonObject json) {

        checkStaticMember();
    }

    public Job(Job other) {
        this.id = other.id;
        this.zid = other.zid;
        this.addressId = other.addressId;
        this.type = other.type;
        this.data = other.data == null ? null : other.data.copy();
        this.priority = other.priority;
        this.jobState = other.jobState;
        this.delay = other.delay;
        // job metrics
        this.createAt = other.createAt;
        this.promoteAt = other.promoteAt;
        this.updatedAt = other.updatedAt;
        this.failedAt = other.failedAt;
        this.startedAt = other.startedAt;
        this.duration = other.duration;
        this.attempts = other.attempts;
        this.maxAttempts = other.maxAttempts;
        this.removeOnComplete = other.removeOnComplete;
        checkStaticMember();
    }

    public Job(String type, JsonObject data) {
        this.type = type;
        this.data = data;
        this.addressId = UUID.randomUUID().toString();
        checkStaticMember();
    }


    public static void setVertx(Vertx v, RedisClient client) {
        vertx = v;
        redisClient = client;
        eventBus = vertx.eventBus();
    }

    private void checkStaticMember() {
        if (Objects.isNull(vertx)) {
            LOGGER.warn("static Vertx instance is not initialized!");
        }
    }
}
