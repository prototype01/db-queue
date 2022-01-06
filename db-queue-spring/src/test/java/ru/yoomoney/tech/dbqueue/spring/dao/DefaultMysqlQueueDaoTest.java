package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.MysqlDatabaseInitializer;

/**
 * @author minho kang
 * @since 05.01.2022
 */
public class DefaultMysqlQueueDaoTest extends QueueDaoTest {
    @BeforeClass
    public static void beforeClass() {
        MysqlDatabaseInitializer.initialize();
    }

    public DefaultMysqlQueueDaoTest() {
        super(
                new MySqlQueueDao(
                        MysqlDatabaseInitializer.jdbcTemplate
                        , MysqlDatabaseInitializer.DEFAULT_SCHEMA
                )
                , MysqlDatabaseInitializer.DEFAULT_TABLE_NAME
                , MysqlDatabaseInitializer.DEFAULT_SCHEMA
                , MysqlDatabaseInitializer.jdbcTemplate
                , MysqlDatabaseInitializer.transactionTemplate
        );
    }
}
