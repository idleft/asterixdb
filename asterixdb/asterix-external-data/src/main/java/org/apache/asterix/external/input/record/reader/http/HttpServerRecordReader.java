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
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpServerRecordReader implements IRecordReader<char[]> {

    private static String ENTRY_POINT = "/datafeed/";
    private static String POST_PARA_NAME = "adm";
    private final int port;
    private LinkedBlockingQueue<String> inputQ;
    private GenericRecord<char[]> record;
    private boolean closed = false;
    private WebManager webManager;

    public HttpServerRecordReader(int port) throws Exception {
        inputQ = new LinkedBlockingQueue<>();
        record = new GenericRecord<>();
        this.port = port;
        webManager = new WebManager();
        configureHttpServer();
        webManager.start();
    }

    private void configureHttpServer() {
        HttpServer webServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(), port);
        webServer.addLet(new HttpFeedServlet(webServer.ctx(), new String[] { ENTRY_POINT }, inputQ));
        webManager.add(webServer);
    }

    @Override
    public boolean hasNext() throws Exception {
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
            webManager.stop();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
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
            webManager.stop();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private class HttpFeedServlet extends AbstractServlet {

        private LinkedBlockingQueue<String> inputQ;

        private void handleMultipleRecords(String admData) throws InterruptedException {
            int p = 0, cnt = 0;
            boolean record = false;
            char[] charBuff = admData.toCharArray();
            for (int iter1 = 0; iter1 < charBuff.length; iter1++) {
                if (charBuff[iter1] == '{') {
                    if (record == false) {
                        p = iter1;
                        record = true;
                    }
                    cnt++;
                } else if (charBuff[iter1] == '}') {
                    cnt--;
                }
                if (cnt == 0) {
                    if (record) {
                        inputQ.put(admData.substring(p, iter1+1)+'\n');
                        record = false;
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
            String admData = request.getParameter(POST_PARA_NAME);
            handleMultipleRecords(admData);
        }

        @Override
        public void handle(IServletRequest request, IServletResponse response) {
            response.setStatus(HttpResponseStatus.OK);
            if (request.getHttpRequest().method() == HttpMethod.POST) {
                try {
                    doPost(request);
                } catch (InterruptedException e) {
                    response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                }
            } else {
                response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
            }
        }
    }
}
