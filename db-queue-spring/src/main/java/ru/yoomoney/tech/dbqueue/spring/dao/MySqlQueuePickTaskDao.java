package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class MySqlQueuePickTaskDao implements QueuePickTaskDao {
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;
    @Nonnull
    private final QueueLocation queueLocation;
    private String pickTaskSql;
    private final String selectSql;
    private final String selectIdSql;
    private FailureSettings failureSettings;

    public MySqlQueuePickTaskDao(
            @Nonnull JdbcOperations jdbcOperations
            , @Nonnull QueueTableSchema queueTableSchema
            , @Nonnull QueueLocation queueLocation
            , @Nonnull FailureSettings failureSettings
    ) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(jdbcOperations));
        this.queueTableSchema = Objects.requireNonNull(queueTableSchema);
        this.queueLocation = Objects.requireNonNull(queueLocation);
        this.failureSettings = Objects.requireNonNull(failureSettings);

        pickTaskSql = createPickTaskSql(queueLocation, failureSettings);
        selectSql = createSelectSql(queueLocation, queueTableSchema);
        selectIdSql = createSelectIdSql(queueLocation, queueTableSchema);

        failureSettings.registerObserver((oldValue, newValue) -> {
            this.failureSettings = newValue;
            pickTaskSql = createPickTaskSql(queueLocation, newValue);
        });
    }

    @Nullable
    @Override
    public TaskRecord pickTask() {
        Optional<Long> selectResult = jdbcTemplate
                .queryForStream(
                        selectIdSql
                        , new MapSqlParameterSource()
                                .addValue("queueId", queueLocation.getQueueId().asString())
                        , (rs, rowNum) -> rs.getLong(queueTableSchema.getIdField())
                )
                .findFirst();

        return selectResult
                .map(this::updateRecord)
                .map(this::selectTaskRecord)
                .orElse(null);
    }

    private long updateRecord(long taskId) {
        MapSqlParameterSource updateParam = new MapSqlParameterSource()
                .addValue("taskId", taskId)
                .addValue("retryInterval", failureSettings.getRetryInterval().getSeconds());

        int updateRows = jdbcTemplate.update(pickTaskSql, updateParam);

        if (updateRows != 1) {
            throw new RuntimeException(new SQLDataException("Not found picking record"));
        }

        return taskId;
    }

    private TaskRecord selectTaskRecord(long taskId) {
        MapSqlParameterSource selectParam = new MapSqlParameterSource()
                .addValue("taskId", taskId);

        Optional<TaskRecord> selectResult = jdbcTemplate
                .queryForStream(
                        selectSql
                        , selectParam
                        , this::toTaskRecord
                )
                .findFirst();

        return selectResult.orElse(null);
    }

    private TaskRecord toTaskRecord(ResultSet rs, int rowNum) {
        try {
            Map<String, String> additionalData = queueTableSchema
                    .getExtFields()
                    .stream()
                    .collect(
                            LinkedHashMap::new,
                            (map, key) -> {
                                try {
                                    map.put(key, rs.getString(key));
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            }, Map::putAll);

            return TaskRecord.builder()
                    .withId(rs.getLong(queueTableSchema.getIdField()))
                    .withCreatedAt(getZonedDateTime(rs, queueTableSchema.getCreatedAtField()))
                    .withNextProcessAt(getZonedDateTime(rs, queueTableSchema.getNextProcessAtField()))
                    .withPayload(rs.getString(queueTableSchema.getPayloadField()))
                    .withAttemptsCount(rs.getLong(queueTableSchema.getAttemptField()))
                    .withReenqueueAttemptsCount(rs.getLong(queueTableSchema.getReenqueueAttemptField()))
                    .withTotalAttemptsCount(rs.getLong(queueTableSchema.getTotalAttemptField()))
                    .withExtData(additionalData)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String createSelectIdSql(
            QueueLocation location
            , QueueTableSchema queueTableSchema
    ) {
        StringJoiner selectFields = new StringJoiner(",")
                .add(queueTableSchema.getIdField());

        queueTableSchema.getExtFields().forEach(selectFields::add);

        return String.format("" +
                        "SELECT %s " +
                        "FROM %s " +
                        "WHERE %s = :queueId " +
                        "  AND %s <= now() " +
                        "  AND %s = 0 " +
                        "ORDER BY %s ASC " +
                        "LIMIT 1 " +
                        "FOR UPDATE ",
                queueTableSchema.getIdField()
                , location.getTableName()
                , queueTableSchema.getQueueNameField()
                , queueTableSchema.getNextProcessAtField()
                , queueTableSchema.getInProcessField()
                , queueTableSchema.getNextProcessAtField()
        );
    }

    private String createSelectSql(
            QueueLocation location
            , QueueTableSchema queueTableSchema
    ) {
        StringJoiner selectFields = new StringJoiner(",")
                .add(queueTableSchema.getIdField())
                .add(queueTableSchema.getCreatedAtField())
                .add(queueTableSchema.getNextProcessAtField())
                .add(queueTableSchema.getPayloadField())
                .add(queueTableSchema.getAttemptField())
                .add(queueTableSchema.getReenqueueAttemptField())
                .add(queueTableSchema.getTotalAttemptField());

        queueTableSchema.getExtFields().forEach(selectFields::add);

        return String.format("" +
                        "SELECT %s " +
                        "FROM %s " +
                        "WHERE %s = :taskId ",
                selectFields,
                location.getTableName(),
                queueTableSchema.getIdField()
        );
    }

    private String createPickTaskSql(
            QueueLocation queueLocation
            , FailureSettings failureSettings
    ) {
        StringJoiner setValues = new StringJoiner(",");
        setValues.add(
                MessageFormat.format(
                        "{0} = {1}"
                        , queueTableSchema.getNextProcessAtField()
                        , getNextProcessTimeSql(failureSettings.getRetryType(), queueTableSchema)
                )
        );
        setValues.add(
                MessageFormat.format(
                        "{0} = {0} + 1"
                        , queueTableSchema.getAttemptField()
                )
        );
        setValues.add(
                MessageFormat.format(
                        "{0} = {0} + 1"
                        , queueTableSchema.getTotalAttemptField()
                )
        );
        setValues.add(
                MessageFormat.format(
                        "{0} = 1"
                        , queueTableSchema.getInProcessField()
                )
        );

        return MessageFormat.format(
                "UPDATE {0} SET {1} WHERE {2} = :taskId"
                , queueLocation.getTableName()
                , setValues.toString()
                , queueTableSchema.getIdField()
        );
    }


    private static ZonedDateTime getZonedDateTime(
            ResultSet rs
            , String time
    ) throws SQLException {
        return ZonedDateTime.ofInstant(rs.getTimestamp(time).toInstant(), ZoneId.systemDefault());
    }

    private static String getNextProcessTimeSql(
            @Nonnull FailRetryType failRetryType
            , @Nonnull QueueTableSchema queueTableSchema
    ) {
        Objects.requireNonNull(failRetryType, "retry type must be not null");
        Objects.requireNonNull(queueTableSchema, "queue table schema must be not null");

        switch (failRetryType) {
            case GEOMETRIC_BACKOFF:
                return String.format("TIMESTAMPADD(SECOND, POWER(2, %s) * :retryInterval , NOW())", queueTableSchema.getAttemptField());
            case ARITHMETIC_BACKOFF:
                return String.format("TIMESTAMPADD(SECOND, (1 + %s * 2) * :retryInterval, NOW())", queueTableSchema.getAttemptField());
            case LINEAR_BACKOFF:
                return "TIMESTAMPADD(SECOND, :retryInterval, NOW())";
            default:
                throw new IllegalStateException("unknown retry type: " + failRetryType);
        }
    }
}
