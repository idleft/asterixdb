/*  1
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
package org.apache.asterix.external.library;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JBuiltinType;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.library.java.base.JDateTime;
import org.apache.asterix.external.library.java.base.JDouble;
import org.apache.asterix.external.library.java.base.JLong;
import org.apache.asterix.external.library.java.base.JObject;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.cglib.core.Local;

public class TweetAnnotationRecord implements IExternalScalarFunction {

    private HashMap<String, Pair<Integer, Integer>> followerList;
    private HashMap<String, Pair<Integer, Integer>> statusList;
    private HashMap<String, Pair<Integer, Integer>> friendsList;
    private HashMap<String, Pair<LocalDateTime, LocalDateTime>> timeRangeList;
    private HashMap<String, Pair<Pair, Pair>> spaceRangeList;
    private String followerPath;
    private String statusPath;
    private String friendsPath;
    private String timePath;
    private String spacePath;
    private JOrderedList fclist;
    private JOrderedList sclist;
    private JOrderedList frlist;
    private JOrderedList trlist;
    private JOrderedList srlist;
    private List<String> functionParameters;
    private int annotateNum;
    private int refreshRate;
    private int refreshCount;
    private String pathPrefix;

    @Override
    public void initialize(IFunctionHelper functionHelper, String nodeInfo) throws Exception {
        if (nodeInfo.startsWith("asterix")) {
            pathPrefix = "/Users/xikuiw/IdeaProjects/TestProject/";
        } else {
            pathPrefix = "/home/xikuiw/decoupled/";
        }

        fclist = new JOrderedList(JBuiltinType.JSTRING);
        sclist = new JOrderedList(JBuiltinType.JSTRING);
        frlist = new JOrderedList(JBuiltinType.JSTRING);
        trlist = new JOrderedList(JBuiltinType.JSTRING);
        srlist = new JOrderedList(JBuiltinType.JSTRING);
        followerList = new HashMap<>();
        statusList = new HashMap<>();
        friendsList = new HashMap<>();
        timeRangeList = new HashMap<>();
        spaceRangeList = new HashMap<>();
        functionParameters = functionHelper.getParameters();
        followerPath = pathPrefix + "/" + functionParameters.get(0);
        statusPath = pathPrefix + "/" + functionParameters.get(1);
        friendsPath = pathPrefix + "/" + functionParameters.get(2);
        timePath = pathPrefix + "/" + functionParameters.get(3);
        spacePath = pathPrefix + "/" + functionParameters.get(4);
        annotateNum = Integer.valueOf(functionParameters.get(5));
        refreshRate = Integer.valueOf(functionParameters.get(6));
        loadLists();
        refreshCount = 0;
    }

    @Override
    public void deinitialize() {
    }

    private void loadLists() throws Exception {
        // Update dictionary
        BufferedReader fr = Files.newBufferedReader(Paths.get(followerPath));
        fr.lines().forEach(line -> {
            String[] items = line.split("\\|");
            followerList.put(items[0], Pair.of(Integer.valueOf(items[1]), Integer.valueOf(items[2])));
        });
        fr.close();

        if (annotateNum > 1) {
            fr = Files.newBufferedReader(Paths.get(statusPath));
            fr.lines().forEach(line -> {
                String[] items = line.split("\\|");
                statusList.put(items[0], Pair.of(Integer.valueOf(items[1]), Integer.valueOf(items[2])));
            });
            fr.close();
        }

        if (annotateNum > 2) {
            fr = Files.newBufferedReader(Paths.get(friendsPath));
            fr.lines().forEach(line -> {
                String[] items = line.split("\\|");
                friendsList.put(items[0], Pair.of(Integer.valueOf(items[1]), Integer.valueOf(items[2])));
            });
            fr.close();
        }

        if (annotateNum > 3) {
            fr = Files.newBufferedReader(Paths.get(timePath));
            fr.lines().forEach(line -> {
                String[] items = line.split("\\|");
                timeRangeList.put(items[0], Pair.of(LocalDateTime.parse(items[1]), LocalDateTime.parse(items[2])));
            });
            fr.close();
        }

        if (annotateNum > 4) {
            fr = Files.newBufferedReader(Paths.get(spacePath));
            fr.lines().forEach(line -> {
                String[] items = line.split("\\|");
                spaceRangeList.put(items[0], Pair.of(Pair.of(Double.valueOf(items[1]), Double.valueOf(items[2])),
                        Pair.of(Double.valueOf(items[3]), Double.valueOf(items[4]))));
            });
            fr.close();
        }
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {

        if (refreshRate > 0) {
            refreshCount = (refreshCount + 1) % refreshRate;
            if (refreshCount == 0) {
                loadLists();
            }
        }

        // Get record
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JRecord userInfo = (JRecord) inputRecord.getValueByName("user");

        // Follower count
        fclist.reset();
        JLong followerCnt = (JLong) userInfo.getValueByName("followers_count");
        for (Map.Entry entry : followerList.entrySet()) {
            Pair range = (Pair<Integer, Integer>) entry.getValue();
            if (followerCnt.getValue() < (Integer) range.getRight()
                    && followerCnt.getValue() >= (Integer) range.getLeft()) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue((String) entry.getKey());
                fclist.add(newField);
            }
        }
        inputRecord.addField("followersClients", fclist);

        // Status count
        if (annotateNum > 1) {
            sclist.reset();
            JLong statusCnt = (JLong) userInfo.getValueByName("status_count");
            for (Map.Entry entry : statusList.entrySet()) {
                Pair range = (Pair<Integer, Integer>) entry.getValue();
                if (statusCnt.getValue() < (Integer) range.getRight()
                        && statusCnt.getValue() >= (Integer) range.getLeft()) {
                    JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                    newField.setValue((String) entry.getKey());
                    sclist.add(newField);
                }
            }
            inputRecord.addField("statusesClients", sclist);
        }

        // Friends count
        if (annotateNum > 2) {
            frlist.reset();
            JLong friendCnt = (JLong) userInfo.getValueByName("friends_count");
            for (Map.Entry entry : friendsList.entrySet()) {
                Pair range = (Pair<Integer, Integer>) entry.getValue();
                if (friendCnt.getValue() < (Integer) range.getRight()
                        && friendCnt.getValue() >= (Integer) range.getLeft()) {
                    JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                    newField.setValue((String) entry.getKey());
                    frlist.add(newField);
                }
            }
            inputRecord.addField("friendsClients", frlist);
        }

        // Time range
        if (annotateNum > 3) {
            trlist.reset();
            JDateTime tweetTime = (JDateTime) inputRecord.getValueByName("created_at");
            for (Map.Entry entry : timeRangeList.entrySet()) {
                Pair range = (Pair<LocalDateTime, LocalDateTime>) entry.getValue();
                LocalDateTime createdTime =
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(tweetTime.getValue()), ZoneOffset.systemDefault());
                if (((LocalDateTime) range.getLeft()).isBefore(createdTime)
                        && ((LocalDateTime) range.getRight()).isAfter(createdTime)) {
                    JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                    newField.setValue((String) entry.getKey());
                    trlist.add(newField);
                }
            }
            inputRecord.addField("timeRangeClients", trlist);
        }

        // Space range
        if (annotateNum > 4) {
            srlist.reset();
            JDouble lat = (JDouble) inputRecord.getValueByName("latitude");
            JDouble lon = (JDouble) inputRecord.getValueByName("longitude");
            for (Map.Entry entry : spaceRangeList.entrySet()) {
                Pair bl = (Pair<Double, Double>) ((Pair<Pair, Pair>) entry.getValue()).getLeft();
                Pair tr = (Pair<Double, Double>) ((Pair<Pair, Pair>) entry.getValue()).getRight();
                if (((Double) bl.getLeft()).compareTo(lat.getValue()) <= 0
                        && ((Double) tr.getLeft()).compareTo(lat.getValue()) > 0
                        && ((Double) bl.getRight()).compareTo(lon.getValue()) <= 0
                        && ((Double) tr.getRight()).compareTo(lon.getValue()) > 0) {
                    JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                    newField.setValue((String) entry.getKey());
                    srlist.add(newField);
                } else {
                    //                System.out.println("WOO?");
                }
            }
            inputRecord.addField("spaceRangeClients", srlist);
        }

        functionHelper.setResult(inputRecord);
    }
}
