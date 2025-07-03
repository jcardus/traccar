package org.traccar.storage;

import com.amazon.athena.jdbc.AthenaDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final AthenaDataSource athenaDataSource;

    @Inject
    public AthenaStorage(Config config, DataSource dataSource, ObjectMapper objectMapper) {
        super(config, dataSource, objectMapper);
        this.config = config;
        this.objectMapper = objectMapper;
        this.athenaDataSource = new AthenaDataSource();
        this.athenaDataSource.setAccessKeyId(System.getenv("ATHENA_AWS_ACCESS_KEY_ID"));
        this.athenaDataSource.setSecretAccessKey(System.getenv("ATHENA_AWS_SECRET_ACCESS_KEY"));
        this.athenaDataSource.setOutputLocation("s3://traccar-athena-output");
        this.athenaDataSource.setDatabase("traccar_positions");
        this.athenaDataSource.setCatalog("AwsDataCatalog");
        this.athenaDataSource.setRegion("us-east-1");
        this.athenaDataSource.setConnectionTest("FALSE");
    }

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
