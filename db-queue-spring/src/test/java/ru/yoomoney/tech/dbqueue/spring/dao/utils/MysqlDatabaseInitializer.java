package ru.yoomoney.tech.dbqueue.spring.dao.utils;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.TestcontainersConfiguration;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Optional;

/**
 * @author minho kang
 * @since 05.01.2022
 */
public class MysqlDatabaseInitializer {
    public static final String DEFAULT_TABLE_NAME = "queue_default";
    public static final String CUSTOM_TABLE_NAME = "queue_custom";
    public static final QueueTableSchema DEFAULT_SCHEMA = QueueTableSchema.builder().build();
    public static final QueueTableSchema CUSTOM_SCHEMA = QueueTableSchema.builder()
            .withIdField("qid")
            .withQueueNameField("qn")
            .withPayloadField("pl")
            .withCreatedAtField("ct")
            .withNextProcessAtField("pt")
            .withAttemptField("att")
            .withReenqueueAttemptField("rat")
            .withTotalAttemptField("tat")
            .withInProcess("ip")
            .withExtFields(Collections.singletonList("trace"))
            .build();

    private static final String MY_SQL_CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  qid    bigint(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,\n" +
            "  qn     varchar(128) NOT NULL,\n" +
            "  pl     longtext,\n" +
            "  ct     datetime DEFAULT CURRENT_TIMESTAMP,\n" +
            "  pt     datetime DEFAULT CURRENT_TIMESTAMP,\n" +
            "  att    int(11)       DEFAULT 0,\n" +
            "  rat    int(11)       DEFAULT 0,\n" +
            "  tat    int(11)       DEFAULT 0,\n" +
            "  it     tinyint(1)    DEFAULT 0,\n" +
            "  trace  varchar(512)  DEFAULT 0\n" +
            ")";


    private static final String MY_SQL_DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                bigint(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,\n" +
            "  queue_name        varchar(128) NOT NULL,\n" +
            "  payload           longtext,\n" +
            "  created_at        datetime DEFAULT CURRENT_TIMESTAMP,\n" +
            "  next_process_at   datetime DEFAULT CURRENT_TIMESTAMP,\n" +
            "  attempt           int(11)                  DEFAULT 0,\n" +
            "  reenqueue_attempt int(11)                  DEFAULT 0,\n" +
            "  total_attempt     int(11)                  DEFAULT 0,\n" +
            "  in_process        tinyint(1)               DEFAULT 0 \n" +
            ")";

    public static JdbcTemplate jdbcTemplate;
    public static TransactionTemplate transactionTemplate;

    public static synchronized void initialize() {
        if (jdbcTemplate != null) {
            return;
        }

        String ryukImage = Optional.ofNullable(System.getProperty("testcontainers.ryuk.container.image"))
                .orElse("quay.io/testcontainers/ryuk:0.2.3");

        TestcontainersConfiguration.getInstance()
                .updateUserConfig("ryuk.container.image", ryukImage);

        String mySqlImage = Optional.ofNullable(System.getProperty("testcontainers.mysql.container.image"))
                .orElse("mysql:5");

        MySQLContainer<?> dbContainer = new MySQLContainer<>(DockerImageName.parse(mySqlImage))
                .withDatabaseName("mysql_test")
                .withUsername("tester")
                .withPassword("tester")
                .withEnv("TZ", ZoneId.systemDefault().getId())
                .withCommand(
                        "--character-set-server=utf8mb4"
                        , "--collation-server=utf8mb4_unicode_ci"
                        , "--lower_case_table_names=1"
                );

        dbContainer.start();

        try {
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setUrl(dbContainer.getJdbcUrl());
            dataSource.setUser(dbContainer.getUsername());
            dataSource.setPassword(dbContainer.getPassword());

            dataSource.setCharacterEncoding("UTF-8");

            jdbcTemplate = new JdbcTemplate(dataSource);
            transactionTemplate = new TransactionTemplate(
                    new DataSourceTransactionManager(dataSource)
            );
            transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
            transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

            // init default scheme
            executeDdl(String.format(MY_SQL_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME));
            executeDdl(MessageFormat.format("ALTER TABLE {0} AUTO_INCREMENT = 1", DEFAULT_TABLE_NAME));

            executeDdl(String.format(MY_SQL_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME));
            executeDdl(MessageFormat.format("ALTER TABLE {0} AUTO_INCREMENT = 1", CUSTOM_TABLE_NAME));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeDdl(String ddl) {
        initialize();
        transactionTemplate.execute(status -> {
           jdbcTemplate.execute(ddl);

           return new Object();
        });
    }
}
