package com.facebook.nifty.server;

import com.facebook.nifty.Pharos;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.testng.annotations.Test;

/**
 * @author zhenchao.wang 2019-07-27 16:22
 * @version 1.0.0
 */
public class PharosTest {

    @Test
    public void testSayHello() throws Exception {
        TSocket socket = new TSocket("localhost", 8080);
        socket.open();
        socket.setTimeout(1000);
        TBinaryProtocol protocol = new TBinaryProtocol(socket);

        Pharos.Client client = new Pharos.Client(protocol);

        System.out.println(client.sayHello("zhenchao"));

        socket.close();
    }
}
