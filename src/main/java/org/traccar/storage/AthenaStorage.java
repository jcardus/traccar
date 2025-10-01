package org.traccar.storage;

import com.amazon.athena.jdbc.AthenaDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import org.traccar.config.Config;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class AthenaStorage extends DatabaseStorage implements DataSource {
    private final ObjectMapper objectMapper;
    private final Config config;
    private final Connection connection;

    @Inject
    public AthenaStorage(Config config, DataSource dataSource, ObjectMapper objectMapper) throws SQLException {
        super(config, dataSource, objectMapper);
        this.config = config;
        this.objectMapper = objectMapper;
        AthenaDataSource athenaDataSource = new AthenaDataSource();
        athenaDataSource.setAccessKeyId(System.getenv("ATHENA_AWS_ACCESS_KEY_ID"));
        athenaDataSource.setSecretAccessKey(System.getenv("ATHENA_AWS_SECRET_ACCESS_KEY"));
        athenaDataSource.setOutputLocation("s3://traccar-athena-output");
        athenaDataSource.setDatabase("traccar_positions");
        athenaDataSource.setCatalog("AwsDataCatalog");
        athenaDataSource.setRegion("us-east-1");
        athenaDataSource.setConnectionTest("FALSE");
        athenaDataSource.setMinQueryExecutionPollingIntervalMillis("200");
        athenaDataSource.setMaxQueryExecutionPollingIntervalMillis("200");
        // athenaDataSource.setResultFetcher("GetQueryResultsStream");
        this.connection = ConnectionWrapper.wrap(athenaDataSource.getConnection());
    }

    @Override
    public <T> Stream<T> getObjectsStream(Class<T> clazz, Request request) throws StorageException {
        if (!Objects.equals(clazz.getAnnotation(StorageName.class).value(), "tc_positions")) {
            return super.getObjectsStream(clazz, request);
        }
        if (!(request.getCondition() instanceof Condition.Binary)) {
            return super.getObjectsStream(clazz, request);
        }

        StringBuilder query = new StringBuilder("SELECT ");
        if (request.getColumns() instanceof Columns.All) {
            query.append('*');
        } else {
            query.append(formatColumns(request.getColumns().getColumns(clazz, "set"), c -> c));
        }
        query.append(" FROM ").append(getStorageName(clazz));
        query.append(formatCondition(request.getCondition()));


        List<Condition> conditions = List.of(
                ((Condition.Binary) request.getCondition()).getFirst(),
                ((Condition.Binary) request.getCondition()).getSecond());
        for (Condition c : conditions) {
            if (c instanceof Condition.Equals condition) {
                query.append(String.format(" AND deviceid_shard='%d' ", (Long) condition.getValue() / 10));
            }
            if (c instanceof Condition.Between condition) {
                if (!Boolean.parseBoolean(System.getenv("ATHENA_ENABLED"))) {
                    return super.getObjectsStream(clazz, request);
                }
                Date fromDate = (Date) condition.getFromValue();
                String from = new SimpleDateFormat("yyyy-MM-dd").format(fromDate);
                query.append(String.format(" AND date >='%s' ", from));
                String to = new SimpleDateFormat("yyyy-MM-dd").format(
                        Date.from(((Date) condition.getToValue()).toInstant().atZone(
                                ZoneId.systemDefault()).plusDays(1).toInstant()));
                query.append(String.format(" AND date <'%s' ", to));
            }
        }
        query.append(formatOrder(request.getOrder()));

        try {
            QueryBuilder builder = QueryBuilder.create(config, this, objectMapper, query.toString());
            List<Object> values = getConditionVariables(request.getCondition());
            for (int index = 0; index < values.size(); index++) {
                builder.setValue(index, values.get(index));
            }
            return builder.executeQueryStreamed(clazz);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public Connection getConnection(String username, String password) {
        return null;
    }

    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
    }

    @Override
    public void setLoginTimeout(int seconds) {
    }

    @Override
    public int getLoginTimeout() {
        return 0;
    }

    @Override
    public Logger getParentLogger() {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
