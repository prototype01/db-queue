package ru.yoomoney.tech.dbqueue.dao;

import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * you can use it to prevent take same record in multi instance
 *
 * @author minho kang
 * @since 06.10.2019
 */
public interface QueueInProcessDao {
    boolean release(@Nonnull QueueLocation location, long taskId);
}
