/*
 * Copyright (C) 2012-2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.nifty.client.socks;

import java.net.InetSocketAddress;

class ScopedSocksServerRunner implements AutoCloseable
{
    private final ExampleSocksServer server;

    public ScopedSocksServerRunner() throws InterruptedException
    {
        this(0);
    }

    public ScopedSocksServerRunner(int serverPort) throws InterruptedException
    {
        server = new ExampleSocksServer(serverPort);
        server.start();
        server.waitForStartup();
    }

    public ExampleSocksServer getServer() {
        return server;
    }

    public InetSocketAddress getLocalAddress() {
        return server.getLocalAddress();
    }

    public int getLocalPort() {
        return server.getLocalPort();
    }

    @Override
    public void close() throws Exception
    {
        server.shutdown();
        server.waitForShutdown();
    }
}
