/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.security.auth;

import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.security.auth.login.Configuration;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import backtype.storm.Config;

/**
 * Interface for Thrift Transport plugin
 */
public interface ITransportPlugin {
    /**
     * Invoked once immediately after construction
     * @param storm_conf Storm configuration 
     * @param login_conf login configuration
     * @param executor_service executor service for server
     */
    void prepare(Map storm_conf, Configuration login_conf, ExecutorService executor_service);
    
    /**
     * Create a server associated with a given port, service handler, and purpose
     * @param port listening port
     * @param processor service handler
     * @param purpose purpose for the thrift server
     * @return server to be binded
     */
    public TServer getServer(int port, TProcessor processor,
            Config.ThriftServerPurpose purpose) 
            throws IOException, TTransportException;

    /**
     * Connect to the specified server via framed transport 
     * @param transport The underlying Thrift transport.
     * @param serverHost server host
     */
    public TTransport connect(TTransport transport, String serverHost) throws IOException, TTransportException;
}
