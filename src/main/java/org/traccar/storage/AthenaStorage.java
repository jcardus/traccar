package org.traccar.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.inject.Inject;
import org.traccar.config.Config;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;

public class AthenaStorage extends DatabaseStorage {
    private final ObjectMapper objectMapper;
    private final Config config;

    @Inject
    public AthenaStorage(Config config, DataSource dataSource, ObjectMapper objectMapper) {
        super(config, dataSource, objectMapper);
        String jdbcUrl = "jdbc:athena://Region=us-east-1";

        // Athena connection properties
        Properties props = new Properties();
        props.setProperty("User", System.getenv("ATHENA_AWS_ACCESS_KEY_ID"));
        props.setProperty("Password", System.getenv("ATHENA_AWS_SECRET_ACCESS_KEY"));
        props.setProperty("OutputLocation", "s3://traccar-athena-positions");
        props.setProperty("Database", "traccar_positions");
        props.setProperty("Catalog", "AwsDataCatalog");

        // HikariCP config
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setDataSourceProperties(props);
        hikariConfig.setDriverClassName("com.amazon.athena.jdbc.AthenaDriver");
        hikariConfig.setMaximumPoolSize(1);
        hikariConfig.setMinimumIdle(0);
        hikariConfig.setIdleTimeout(0);
        hikariConfig.setInitializationFailTimeout(-1);

        athenaDataSource = new HikariDataSource(hikariConfig);
        this.config = config;
        this.objectMapper = objectMapper;
    }

    private final DataSource athenaDataSource;



    @Override
    public <T> List<T> getObjects(Class<T> clazz, Request request) throws StorageException {
        if (!Objects.equals(clazz.getAnnotation(StorageName.class).value(), "tc_positions")) {
            return super.getObjects(clazz, request);
        }
        if (!(request.getCondition() instanceof Condition.Binary)) {
            return super.getObjects(clazz, request);
        }

        StringBuilder query = new StringBuilder("SELECT ");
        if (request.getColumns() instanceof Columns.All) {
            query.append('*');
        } else {
            query.append(formatColumns(request.getColumns().getColumns(clazz, "set"), c -> c));
        }
        query.append(" FROM ").append(getStorageName(clazz));
        query.append(formatCondition(request.getCondition()));
        for (Map.Entry<String, Object> variable : getConditionVariables(request.getCondition()).entrySet()) {
            switch(variable.getKey()) {
                case "deviceId":
                    query.append(String.format(" AND deviceid_shard='%d' ", (Long)variable.getValue()/10));
                    break;
                case "from":
                    String from = new SimpleDateFormat("yyyy-MM-dd").format((Date)variable.getValue());
                    query.append(String.format(" AND date >='%s' ", from));
                    break;
                case "to":
                    String to = new SimpleDateFormat("yyyy-MM-dd").format(Date.from(((Date)variable.getValue()).toInstant().atZone(ZoneId.systemDefault()).plusDays(1).toInstant()));
                    query.append(String.format(" AND date <'%s' ", to));
                    break;
            }
        }

        query.append(formatOrder(request.getOrder()));
        try {
            QueryBuilder builder = QueryBuilder.create(config, athenaDataSource, objectMapper, query.toString());
            for (Map.Entry<String, Object> variable : getConditionVariables(request.getCondition()).entrySet()) {
                builder.setValue(variable.getKey(), variable.getValue());
            }
            return builder.executeQuery(clazz);
        } catch (SQLException e) {
            throw new StorageException(e);
        }
    }


}
