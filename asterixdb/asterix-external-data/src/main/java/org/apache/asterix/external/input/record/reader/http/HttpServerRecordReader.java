/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.asterix.external.input.record.reader.http;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.AuthenticatedHttpServer;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;
import org.apache.hyracks.http.server.authenticator.BasicAuthenticator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpServerRecordReader implements IRecordReader<char[]> {

    public static final Logger LOGGER = LogManager.getLogger();

    private static final String DEFAULT_ENTRY_POINT = "/";
    private LinkedBlockingQueue<String> inputQ;
    private GenericRecord<char[]> record;
    private boolean closed = false;
    private WebManager webManager;
    private HttpServer webServer;

    public HttpServerRecordReader(int port, String username, String password, String entryPoint) throws Exception {
        this.inputQ = new LinkedBlockingQueue<>();
        this.record = new GenericRecord<>();
        webManager = new WebManager();
        if (username == null || password == null) {
            webServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(), port);
        } else {
            webServer = new AuthenticatedHttpServer(webManager.getBosses(), webManager.getWorkers(), port,
                    new BasicAuthenticator(username, password));
        }
        webServer.addServlet(new HttpFeedServlet(webServer.ctx(),
                new String[] { entryPoint == null ? DEFAULT_ENTRY_POINT : entryPoint }, inputQ));
        webManager.add(webServer);
        webManager.start();
    }

    @Override
    public boolean hasNext() {
        return !closed;
    }

    @Override
    public IRawRecord<char[]> next() throws IOException, InterruptedException {
        String srecord = inputQ.poll();
        if (srecord == null) {
            return null;
        }
        record.set(srecord.toCharArray());
        return record;
    }

    @Override
    public boolean stop() {
        try {
            close();
        } catch (Exception e) {
            LOGGER.error(e);
            return false;
        }
        return true;
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        // do nothing
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) throws HyracksDataException {
        // do nothing
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    @Override
    public void close() throws IOException {
        try {
            if (!closed) {
                webManager.stop();
                closed = true;
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private class HttpFeedServlet extends AbstractServlet {

        private LinkedBlockingQueue<String> inputQ;

        private void splitIntoRecords(String admData) throws InterruptedException {
            int p = 0, cnt = 0;
            boolean inRecord = false;
            char[] charBuff = admData.toCharArray();
            for (int iter1 = 0; iter1 < charBuff.length; iter1++) {
                if (charBuff[iter1] == '{') {
                    if (inRecord == false) {
                        p = iter1;
                        inRecord = true;
                    }
                    cnt++;
                } else if (charBuff[iter1] == '}') {
                    cnt--;
                }
                if (cnt == 0) {
                    if (inRecord) {
                        inputQ.put(admData.substring(p, iter1 + 1) + '\n');
                        inRecord = false;
                    }
                    p = iter1;
                }
            }
        }

        public HttpFeedServlet(ConcurrentMap<String, Object> ctx, String[] paths, LinkedBlockingQueue<String> inputQ) {
            super(ctx, paths);
            this.inputQ = inputQ;
        }

        private void doPost(IServletRequest request) throws InterruptedException {
            splitIntoRecords(request.getHttpRequest().content().toString(StandardCharsets.UTF_8));
        }

        @Override
        public void handle(IServletRequest request, IServletResponse response) {
            if (request.getHttpRequest().method() == HttpMethod.POST) {
                try {
                    doPost(request);
                    response.setStatus(HttpResponseStatus.OK);
                } catch (InterruptedException e) {
                    LOGGER.log(Level.INFO, "exception thrown for " + request, e);
                    response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                }
            } else {
                response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
            }
        }
    }
}
