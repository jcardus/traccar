/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.forward;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class NetworkForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkForwarder.class);

    private InetAddress destination;
    private final DatagramSocket connectionUdp;
    private final Map<InetSocketAddress, List<Socket>> connectionsTcp = new HashMap<>();

    @Inject
    public NetworkForwarder(Config config) throws IOException {
        destination = InetAddress.getByName(config.getString(Keys.SERVER_FORWARD));
        connectionUdp = new DatagramSocket();
    }

    public void forward(InetSocketAddress source, int port, boolean datagram, byte[] data) {
        if (port == 5023) {
            sendTo(source, "nt20.stc.srv.br", 10107, datagram, data);
            sendTo(source, "serv8.rastrosystem.com.br", 5023, datagram, data);
        } else {
            sendTo(source, "jsrastreamento.stctecnologia.com.br", port, datagram, data);
        }
    }

    private void sendTo(InetSocketAddress source, String host, int port, boolean datagram, byte[] data) {
        try {
            InetAddress target = InetAddress.getByName(host);
            if (datagram) {
                connectionUdp.send(new DatagramPacket(data, data.length, target, port));
            } else {
                List<Socket> sockets = connectionsTcp.computeIfAbsent(source, k -> new ArrayList<>());
                Socket connectionTcp = null;
                for (Socket s : sockets) {
                    if (!s.isClosed()
                            && s.getInetAddress().equals(target)
                            && s.getPort() == port) {
                        connectionTcp = s;
                        break;
                    }
                }
                if (connectionTcp == null) {
                    connectionTcp = new Socket(target, port);
                    sockets.add(connectionTcp);
                }
                connectionTcp.getOutputStream().write(data);
            }
        } catch (IOException e) {
            LOGGER.warn("Network forwarding error", e);
        }
    }

    public void disconnect(InetSocketAddress source) {
        List<Socket> sockets = connectionsTcp.remove(source);
        if (sockets != null) {
            for (Socket connectionTcp : sockets) {
                try {
                    connectionTcp.close();
                } catch (IOException e) {
                    LOGGER.warn("Connection close error", e);
                }
            }
        }
    }

}
