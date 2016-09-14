package io.vertx.paulzhang.kue.queue;

/**
 * Created by PaulZhang on 2016/9/13.
 */
public enum JobState {

    INACTIVE,
    ACTIVE,
    COMPLETE,
    FAILED,
    DELAYED
}
