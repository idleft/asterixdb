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
package org.apache.asterix.external.generator;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Map;

public class DataGeneratorInfo {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");

    public final DataGenerator.Date startDate;
    public final DataGenerator.Date endDate;
    public final String[] lastNames = DataGenerator.lastNames;
    public final String[] firstNames = DataGenerator.firstNames;
    public final String[] vendors = DataGenerator.vendors;
    public final String[] jargon = DataGenerator.jargon;
    public final int slat, elat, slong, elong;
    public final int numFriends, statusesCount, followersCount;

    public DataGeneratorInfo(LocalDate sdate, LocalDate edate, int slat, int slong, int elat, int elong, int numFriends,
            int statusesCount, int followersCount) {
        startDate = new DataGenerator.Date(sdate.getMonthValue(), sdate.getDayOfMonth(), sdate.getYear());
        endDate = new DataGenerator.Date(edate.getMonthValue(), edate.getDayOfMonth(), edate.getYear());
        this.slat = slat;
        this.elat = elat;
        this.slong = slong;
        this.elong = elong;
        this.numFriends = numFriends;
        this.statusesCount = statusesCount;
        this.followersCount = followersCount;
    }

    private static int getIntValue(Map<String, String> configs, String key) {
        return Integer.valueOf(configs.getOrDefault(key, "0"));
    }

    public static DataGeneratorInfo getDataGeneratorInfoFromConfigs(Map<String, String> configs) {
        LocalDate sdate, edate;
        sdate = LocalDate.parse(configs.getOrDefault("sdate", "1988-12-26"));
        edate = LocalDate.parse(configs.getOrDefault("edate", "1989-09-07"));
        return new DataGeneratorInfo(sdate, edate, getIntValue(configs, "slat"), getIntValue(configs, "slong"),
                getIntValue(configs, "elat"), getIntValue(configs, "elong"), getIntValue(configs, "numFriends"),
                getIntValue(configs, "statusesCount"), getIntValue(configs, "followersCount"));
    }
}
