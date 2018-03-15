package com.wallstreetcn.flume;

import com.google.common.base.Strings;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.jooq.Batch;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * MySQLSink fetches data from channel, and load them to the MySQL.
 * <p>
 * The configuration example,
 * agent1.sinks.k1.type = com.wallstreetcn.flume.JDBCSink
 * agent1.sinks.k1.driver = com.mysql.jdbc.Driver
 * agent1.sinks.k1.connectionURL = jdbc:mysql://mysql-address/test
 * agent1.sinks.k1.mapping = field1:body.app_id,field2:header.field2
 * agent1.sinks.k1.user = root
 * agent1.sinks.k1.password = root
 * agent1.sinks.k1.table = test
 * agent1.sinks.k1.sqlDialect = MySQL
 */
public class JDBCSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

    private static final String CONF_DRIVER = "driver";
    private static final String CONF_CONNECTION_URL = "connectionURL";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String CONF_MAPPING = "mapping";
    private static final String CONF_USER = "user";
    private static final String CONF_PASSWORD = "password";
    private static final String CONF_TABLE = "table";
    private static final String CONF_SQL_DIALECT = "sqlDialect";
    private static final String CONF_BODY_FORMAT = "bodyFormat";
    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final String DEFAULT_BODY_FORMAT = "json";

    private int batchSize;
    private String bodyFormat;
    private String mapping;
    private String tableName;

    private Connection connection;
    private DSLContext dslContext;
    private SinkCounter counter;

    @Override
    public void configure(Context context) {
        final String driver = context.getString(CONF_DRIVER);
        final String connectionURL = context.getString(CONF_CONNECTION_URL);

        this.batchSize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.bodyFormat = context.getString(CONF_BODY_FORMAT, DEFAULT_BODY_FORMAT);
        this.tableName = context.getString(CONF_TABLE);
        if (this.tableName == null || this.tableName.isEmpty()) {
            throw new JDBCSinkException("table name must not be empty");
        }

        try {
            LOG.debug("start to initialize driver: " + driver);
            Class.forName(driver).newInstance();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new JDBCSinkException(e);
        }

        String username = context.getString(CONF_USER);
        String password = context.getString(CONF_PASSWORD);

        try {
            if (Strings.isNullOrEmpty(username) || Strings.isNullOrEmpty(password)) {
                connection = DriverManager.getConnection(connectionURL);
            } else {
                connection = DriverManager.getConnection(connectionURL, username, password);
            }
        } catch (SQLException e) {
            throw new JDBCSinkException(e);
        }

        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new JDBCSinkException(e);
        }

        final ConnectionProvider cp = new DefaultConnectionProvider(connection);
        final SQLDialect sqlDialect = SQLDialect.valueOf(context.getString(CONF_SQL_DIALECT).toUpperCase(Locale.ENGLISH));

        this.dslContext = DSL.using(cp, sqlDialect);
        this.mapping = context.getString(CONF_MAPPING);
        if (this.mapping == null || this.mapping.isEmpty()) {
            throw new JDBCSinkException("sql string must not be empty");
        }

        if (this.counter == null) {
            this.counter = new SinkCounter(this.getName());
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel ch = getChannel();
        Status status = Status.READY;
        Transaction txn = ch.getTransaction();

        txn.begin();

        try {
            long count = 0;

            List<Event> events = new ArrayList<>();
            for (; count < batchSize; count++) {
                Event event = ch.take();
                if (event == null) {
                    if (count == 0) {
                        status = Status.BACKOFF;
                        counter.incrementBatchEmptyCount();
                    } else {
                        counter.incrementBatchUnderflowCount();
                    }
                    break;
                }
                events.add(event);
            }

            LOG.debug("Started to batch " + events.size() + " events.");

            Batch batch = dslContext.batch(MappingQuery.render(
                    this.dslContext,
                    this.tableName,
                    mapping,
                    events,
                    this.bodyFormat
            ));
            batch.execute();

            connection.commit();
            txn.commit();

            counter.addToEventDrainSuccessCount(count);
            LOG.info("Success to batch " + events.size() + " events.");
        } catch (DataAccessException | SQLException e) {
            String errorMsg = "Failed to publish events";
            LOG.error("Failed to insert into database", e);
            counter.incrementConnectionClosedCount();

            try {
                connection.rollback();
            } catch (SQLException e1) {
                LOG.error("Failed to rollback", e1);
            } finally {
                txn.rollback();
            }
            throw new EventDeliveryException(errorMsg, e);
        } finally {
            txn.close();
        }

        return status;
    }

    @Override
    public synchronized void start() {
        counter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        counter.stop();
        LOG.info("JDBC Sink {} stopped. Metrics: {}", getName(), counter);
        super.stop();
    }
}
