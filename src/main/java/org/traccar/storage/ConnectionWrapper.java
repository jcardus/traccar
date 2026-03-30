package org.traccar.storage;
import java.lang.reflect.Proxy;
import java.sql.Connection;

final class ConnectionWrapper {
    private ConnectionWrapper() {
    }

    public static Connection wrap(Connection originalConnection) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    //original close takes 2 seconds...
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    return method.invoke(originalConnection, args);
                }
        );
    }
}
