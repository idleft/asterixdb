/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.common;

import org.apache.commons.logging.Log;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.RotateRunFileReader;
import org.apache.hyracks.dataflow.common.io.RotateRunFileWriter;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.tests.util.InputFrameGenerator;
import org.apache.hyracks.tests.util.OutputFrameVerifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static junit.framework.TestCase.fail;

public class RotateRunFileTest {

    private static int DEFAULT_FRAME_SIZE = 32768;

    private static final Logger LOGGER = Logger.getLogger(RotateRunFileTest.class.getName());

    private RecordDescriptor recordDesc = new RecordDescriptor(
            new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

    @Test
    public void OneProducerOneConsumerChasingTest() throws HyracksDataException {
        int frameN = 300;
        int framePerFile = 10;
        int bufferSize = 16;
        IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
        InputFrameGenerator frameGenerator = new InputFrameGenerator(DEFAULT_FRAME_SIZE);
        List<IFrame>[] inputFrames;
        List<Object[]> expectedAnswers;
        inputFrames = new List[frameN];
        expectedAnswers = new ArrayList<>();
        VSizeFrame outputBuffer = new VSizeFrame(ctx);

        prepareTestFrames(inputFrames, expectedAnswers, frameN, frameGenerator);
        RotateRunFileWriter writer = new RotateRunFileWriter("Test-01", ctx, bufferSize, framePerFile,
                DEFAULT_FRAME_SIZE);
        writer.open();
        RotateRunFileReader reader = writer.getReader();
        reader.open();
        OutputFrameVerifier outputFrameVerifier = new OutputFrameVerifier(recordDesc, expectedAnswers);
        for (int iter1 = 0; iter1 < frameN; iter1++) {
            List<IFrame> framesInCurrentRun = inputFrames[iter1];
            for (int iter2 = 0; iter2 < framesInCurrentRun.size(); iter2++) {
                writer.nextFrame(framesInCurrentRun.get(iter2).getBuffer());
            }
            for (int iter2 = 0; iter2 < framesInCurrentRun.size(); iter2++) {
                reader.nextFrame(outputBuffer);
                outputFrameVerifier.nextFrame(outputBuffer.getBuffer());
            }
        }
    }

    @Test
    public void OneProducerOneConsumerByStageTest() throws HyracksDataException {
        int frameN = 150;
        int framePerFile = 10;
        int bufferSize = 16;
        IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
        InputFrameGenerator frameGenerator = new InputFrameGenerator(DEFAULT_FRAME_SIZE);
        List<IFrame>[] inputFrames;
        List<Object[]> expectedAnswers;
        inputFrames = new List[frameN];
        expectedAnswers = new ArrayList<>();
        VSizeFrame outputBuffer = new VSizeFrame(ctx);

        prepareTestFrames(inputFrames, expectedAnswers, frameN, frameGenerator);
        RotateRunFileWriter writer = new RotateRunFileWriter("Test-02", ctx, bufferSize, framePerFile,
                DEFAULT_FRAME_SIZE);
        writer.open();
        RotateRunFileReader reader = writer.getReader();
        reader.open();
        OutputFrameVerifier outputFrameVerifier = new OutputFrameVerifier(recordDesc, expectedAnswers);
        for (int iter1 = 0; iter1 < frameN; iter1++) {
            List<IFrame> framesInCurrentRun = inputFrames[iter1];
            for (int iter2 = 0; iter2 < framesInCurrentRun.size(); iter2++) {
                writer.nextFrame(framesInCurrentRun.get(iter2).getBuffer());
            }
        }
        for (int iter1 = 0; iter1 < frameN; iter1++) {
            List<IFrame> framesInCurrentRun = inputFrames[iter1];
            for (int iter2 = 0; iter2 < framesInCurrentRun.size(); iter2++) {
                reader.nextFrame(outputBuffer);
                outputFrameVerifier.nextFrame(outputBuffer.getBuffer());
            }
        }
    }

    @Test
    public void OneProducerOneConsumerByStageBlockingTest() throws HyracksDataException {
        int frameN = 200;
        int framePerFile = 10;
        int bufferSize = 16;
        IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
        InputFrameGenerator frameGenerator = new InputFrameGenerator(DEFAULT_FRAME_SIZE);
        List<IFrame>[] inputFrames;
        List<Object[]> expectedAnswers;
        inputFrames = new List[frameN];
        expectedAnswers = new ArrayList<>();
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        prepareTestFrames(inputFrames, expectedAnswers, frameN, frameGenerator);
        RotateRunFileWriter writer = new RotateRunFileWriter("Test-03", ctx, bufferSize, framePerFile,
                DEFAULT_FRAME_SIZE);
        writer.open();
        RotateRunFileReader reader = writer.getReader();
        reader.open();
        try {
            executorService.submit(() -> {
                for (int iter1 = 0; iter1 < frameN; iter1++) {
                    List<IFrame> framesInCurrentRun = inputFrames[iter1];
                    for (int iter2 = 0; iter2 < framesInCurrentRun.size(); iter2++) {
                        writer.nextFrame(framesInCurrentRun.get(iter2).getBuffer());
                    }
                }
                return null;
            }).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof TimeoutException);
        }
        executorService.shutdown();
    }

    @Test
    public void OneProducerOneConsumerConcurrentTest() throws HyracksDataException {
        int frameN = 10000;
        int framePerFile = 10;
        int bufferSize = 16;
        IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
        InputFrameGenerator frameGenerator = new InputFrameGenerator(DEFAULT_FRAME_SIZE);
        List<IFrame>[] inputFrames;
        List<Object[]> expectedAnswers;
        inputFrames = new List[frameN];
        expectedAnswers = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        prepareTestFrames(inputFrames, expectedAnswers, frameN, frameGenerator);
        RotateRunFileWriter writer = new RotateRunFileWriter("Test-04", ctx, bufferSize, framePerFile,
                DEFAULT_FRAME_SIZE);
        writer.open();
        RotateRunFileReader reader = writer.getReader();
        reader.open();
        OutputFrameVerifier outputFrameVerifier = new OutputFrameVerifier(recordDesc, expectedAnswers);
        Future writerFuture, readerFuture;

        try {
            writerFuture = executorService.submit(() -> {
                for (int iter1 = 0; iter1 < frameN; iter1++) {
                    List<IFrame> framesInCurrentRun = inputFrames[iter1];
                    for (int iter2 = 0; iter2 < framesInCurrentRun.size(); iter2++) {
                        LOGGER.finest("Writer: Attempt to write " + iter1);
                        writer.nextFrame(framesInCurrentRun.get(iter2).getBuffer());
                        LOGGER.finest("Writer: Wrote " + iter1);
                    }
                }
                writer.close();
                return null;
            });
            readerFuture = executorService.submit(new RotateTestReader(0, reader, ctx, outputFrameVerifier));
            writerFuture.get();
            readerFuture.get();
        } catch (Exception e) {
            fail();
        }
        executorService.shutdown();
    }

    @Test
    public void OneProducerMultipleConsumerConcurrentTest() throws HyracksDataException {
        int frameN = 10000;
        int framePerFile = 10;
        int bufferSize = 16;
        int readerN = 20;
        IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
        InputFrameGenerator frameGenerator = new InputFrameGenerator(DEFAULT_FRAME_SIZE);
        List<IFrame>[] inputFrames;
        List<Object[]> expectedAnswers;
        inputFrames = new List[frameN];
        expectedAnswers = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(30);

        prepareTestFrames(inputFrames, expectedAnswers, frameN, frameGenerator);
        RotateRunFileWriter writer = new RotateRunFileWriter("Test-05", ctx, bufferSize, framePerFile,
                DEFAULT_FRAME_SIZE);
        writer.open();
        List<RotateRunFileReader> readers = new ArrayList<>();
        for (int iter1 = 0; iter1 < readerN; iter1++) {
            readers.add(writer.getReader());
            readers.get(iter1).open();
        }
        Future writerFuture;
        List<Future> readerFutures = new ArrayList<>();
        try {
            writerFuture = executorService.submit(() -> {
                for (int iter1 = 0; iter1 < frameN; iter1++) {
                    List<IFrame> framesInCurrentRun = inputFrames[iter1];
                    for (int iter2 = 0; iter2 < framesInCurrentRun.size(); iter2++) {
                        LOGGER.log(Level.FINEST, "Writer: Attempt to write " + iter1);
                        writer.nextFrame(framesInCurrentRun.get(iter2).getBuffer());
                        LOGGER.log(Level.FINEST, "Writer: Wrote " + iter1);
                    }
                }
                writer.close();
                return null;
            });
            for (RotateRunFileReader reader : readers) {
                OutputFrameVerifier outputFrameVerifier = new OutputFrameVerifier(recordDesc, expectedAnswers);
                RotateTestReader rotateTestReader = new RotateTestReader(readers.indexOf(reader), reader, ctx,
                        outputFrameVerifier);
                readerFutures.add(executorService.submit(rotateTestReader));
            }
            writerFuture.get();
            for (Future readerFuture : readerFutures) {
                readerFuture.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        executorService.shutdown();
    }

    private class RotateTestReader implements Runnable {

        private RotateRunFileReader reader;
        private VSizeFrame outputFrame;
        private OutputFrameVerifier outputFrameVerifier;
        private int readerId;

        public RotateTestReader(int readerId, RotateRunFileReader reader, IHyracksTaskContext ctx,
                OutputFrameVerifier outputFrameVerifier) throws HyracksDataException {
            this.readerId = readerId;
            this.reader = reader;
            this.outputFrame = new VSizeFrame(ctx, DEFAULT_FRAME_SIZE);
            this.outputFrameVerifier = outputFrameVerifier;
        }

        @Override
        public void run() {
            try {
                int readFrameCtr = 0;
                while (reader.nextFrame(outputFrame)) {
                    LOGGER.log(Level.FINEST, "Reader " + readerId + " reads file " + reader.getCurrentFileIdx());
                    outputFrameVerifier.nextFrame(outputFrame.getBuffer());
                    LOGGER.log(Level.FINEST, "Reader: Read " + readFrameCtr++);
                }
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    private void prepareTestFrames(List<IFrame>[] inputFrames, List<Object[]> answer, int frameNum,
            InputFrameGenerator frameGenerator) throws HyracksDataException {
        for (int iter1 = 0; iter1 < frameNum; iter1++) {
            List<Object[]> inputObjects = new ArrayList<>();
            generateRecordStream(inputObjects, recordDesc, iter1 * 100, (iter1 + 1) * 100, 1);
            inputFrames[iter1] = frameGenerator.generateDataFrame(recordDesc, inputObjects);
        }
        generateRecordStream(answer, recordDesc, 0, frameNum * 100, 1);
    }

    private void generateRecordStream(List<Object[]> inputs, RecordDescriptor recordDesc, int start, int end,
            int step) {
        for (int i = start; i < end; i += step) {
            Object[] obj = new Object[recordDesc.getFieldCount()];
            for (int f = 0; f < recordDesc.getFieldCount(); f++) {
                obj[f] = i;
            }
            inputs.add(obj);
        }
    }
}
