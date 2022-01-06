package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.dao.QueueInProcessDao;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Database access object to manage tasks in the queue for MySql database type.
 * @author minho kang
 * @since 05.01.2022
 */
public class MySqlQueueDao implements QueueDao, QueueInProcessDao {
    @Nonnull
    private final NamedParameterJdbcTemplate jdbcTemplate;
    @Nonnull
    private final QueueTableSchema queueTableSchema;

    private final Map<QueueLocation, String> enqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> deleteSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> reenqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> releaseSqlCache = new ConcurrentHashMap<>();

    public MySqlQueueDao(
            @Nonnull JdbcOperations jdbcOperations
            , @Nonnull QueueTableSchema queueTableSchema
    ) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(
                requireNonNull(
                        jdbcOperations
                        , "jdbc template can't be null"
                )
        );

        this.queueTableSchema = Objects.requireNonNull(
                queueTableSchema
                , "table schema can't be null"
        );
    }

    @Override
    public long enqueue(
            @Nonnull QueueLocation location
            , @Nonnull EnqueueParams<String> enqueueParams
    ) {
        requireNonNull(location);
        requireNonNull(enqueueParams);

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueId().asString())
                .addValue("payload", enqueueParams.getPayload())
                .addValue("executionDelay", enqueueParams.getExecutionDelay().getSeconds());

        queueTableSchema.getExtFields().forEach(paramName -> params.addValue(paramName, null));
        enqueueParams.getExtData().forEach(params::addValue);

        String sql = enqueueSqlCache.computeIfAbsent(
                location
                , this::createEnqueueSql
        );

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(sql, params, keyHolder);

        return requireNonNull(keyHolder.getKey())
                .longValue();
    }

    @Override
    public boolean deleteTask(
            @Nonnull QueueLocation location
            , long taskId
    ) {
        requireNonNull(location);

        int updatedRows = jdbcTemplate.update(
                deleteSqlCache.computeIfAbsent(
                        location
                        , this::createDeleteSql
                ),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString()));

        return updatedRows != 0;
    }

    @Override
    public boolean reenqueue(
            @Nonnull QueueLocation location
            , long taskId
            , @Nonnull Duration executionDelay
    ) {
        requireNonNull(location);
        requireNonNull(executionDelay);

        int updatedRows = jdbcTemplate.update(
                reenqueueSqlCache.computeIfAbsent(
                        location
                        , this::createReenqueueSql
                ),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString())
                        .addValue("executionDelay", executionDelay.getSeconds()));

        return updatedRows != 0;
    }

    @Override
    public boolean release(
            @Nonnull QueueLocation location
            , long taskId
    ) {
        requireNonNull(location);

        int updatedRows = jdbcTemplate.update(
                releaseSqlCache.computeIfAbsent(
                        location
                        , this::createReleaseSql
                ),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString())
        );

        return updatedRows != 0;
    }

    private String createEnqueueSql(
            @Nonnull QueueLocation location
    ) {
        // mysql does not support sequence
        StringJoiner columnNames = new StringJoiner(",");
        columnNames.add(queueTableSchema.getQueueNameField());
        columnNames.add(queueTableSchema.getPayloadField());
        columnNames.add(queueTableSchema.getNextProcessAtField());
        columnNames.add(queueTableSchema.getReenqueueAttemptField());
        columnNames.add(queueTableSchema.getTotalAttemptField());

        queueTableSchema
                .getExtFields()
                .forEach(columnNames::add);

        StringJoiner valueNames = new StringJoiner(",");
        valueNames.add(":queueName");
        valueNames.add(":payload");
        valueNames.add("TIMESTAMPADD(SECOND,:executionDelay,NOW())");
        valueNames.add("0");
        valueNames.add("0");

        queueTableSchema
                .getExtFields()
                .forEach(s -> columnNames.add(""));

        return MessageFormat.format(
                "INSERT INTO {0} ({1}) VALUES ({2})"
                , location.getTableName()
                , columnNames.toString()
                , valueNames.toString()
        );
    }

    private String createDeleteSql(
            @Nonnull QueueLocation location
    ) {
        return MessageFormat.format(
                "DELETE FROM {0} WHERE {1} = :queueName AND {2} = :id"
                , location.getTableName()
                , queueTableSchema.getQueueNameField()
                , queueTableSchema.getIdField()
        );
    }

    private String createReenqueueSql(
            @Nonnull QueueLocation location
    ) {
        StringJoiner setValues = new StringJoiner(",");
        setValues.add(
                MessageFormat.format(
                        "{0} = TIMESTAMPADD(SECOND, :executionDelay, NOW())"
                        , queueTableSchema.getNextProcessAtField())
        );
        setValues.add(
                MessageFormat.format(
                        "{0} = 0"
                        , queueTableSchema.getAttemptField())
        );
        setValues.add(
                MessageFormat.format(
                        "{0} = {1} + 1"
                        , queueTableSchema.getReenqueueAttemptField()
                        , queueTableSchema.getReenqueueAttemptField())
        );
        setValues.add(
                MessageFormat.format(
                        "{0} = 0"
                        , queueTableSchema.getInProcessField())
        );

        return MessageFormat.format(
                "UPDATE {0} SET {1} WHERE {2} = :id AND {3} = :queueName"
                , location.getTableName()
                , setValues.toString()
                , queueTableSchema.getIdField()
                , queueTableSchema.getQueueNameField()
        );
    }

    private String createReleaseSql(
            @Nonnull QueueLocation location
    ) {
        StringJoiner setValues = new StringJoiner(",");
        setValues.add(
                MessageFormat.format(
                        "{0} = 0"
                        , queueTableSchema.getInProcessField())
        );

        return MessageFormat.format(
                "UPDATE {0} SET {1} WHERE {2} = :id AND {3} = :queueName"
                , location.getTableName()
                , setValues.toString()
                , queueTableSchema.getIdField()
                , queueTableSchema.getQueueNameField()
        );
    }
}
