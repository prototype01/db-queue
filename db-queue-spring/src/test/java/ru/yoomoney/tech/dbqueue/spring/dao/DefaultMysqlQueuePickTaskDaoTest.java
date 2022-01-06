package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.MysqlDatabaseInitializer;

public class DefaultMysqlQueuePickTaskDaoTest extends QueuePickTaskDaoTest {
    @BeforeClass
    public static void beforeClass() {
        MysqlDatabaseInitializer.initialize();
    }

    public DefaultMysqlQueuePickTaskDaoTest() {
        super(
                new MySqlQueueDao(
                        MysqlDatabaseInitializer.jdbcTemplate
                        , MysqlDatabaseInitializer.DEFAULT_SCHEMA
                )
                , (queueLocation, failureSettings) -> new MySqlQueuePickTaskDao(
                        MysqlDatabaseInitializer.jdbcTemplate
                        , MysqlDatabaseInitializer.DEFAULT_SCHEMA
                        , queueLocation
                        , failureSettings
                )
                , MysqlDatabaseInitializer.DEFAULT_TABLE_NAME
                , MysqlDatabaseInitializer.DEFAULT_SCHEMA
                , MysqlDatabaseInitializer.jdbcTemplate
                , MysqlDatabaseInitializer.transactionTemplate
        );
    }

    @Override
    protected String currentTimeSql() {
        return "CURRENT_TIMESTAMP";
    }
}
