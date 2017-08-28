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

package org.apache.hyracks.control.nc.io.profiling;

import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class DiskFreeSizeCalculatorFactory {

    public static IDiskFreeSpaceCalculator getDiskFreeSpaceCalculator() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix") || osName.contains("mac")) {
            return new DFDiskFreeSpaceCalculator();
        } else {
            return new DefaultDiskFreeSpaceCalculator();
        }
    }
}

class DFDiskFreeSpaceCalculator implements IDiskFreeSpaceCalculator {

    private Pair<String, Long> getDeviceFreeSpace(String path) throws IOException {
        BufferedReader resultReader = exec("df " + path);
        String line;
        String[] cols = null;
        int lineCounter = 0;
        while ((line = resultReader.readLine()) != null) {
            if (lineCounter == 1) {
                cols = line.split(" +");
                break;
            }
            lineCounter++;
        }
        return cols == null ? Pair.of("", 0l) : Pair.of(cols[0], Long.valueOf(cols[3]));
    }

    private BufferedReader exec(String command) throws IOException {
        Process p = Runtime.getRuntime().exec(command);
        return new BufferedReader(new InputStreamReader(p.getInputStream()));
    }

    @Override
    public Pair<String, Long> getFreeDiskSize(String path) {
        try {
            return getDeviceFreeSpace(path);
        } catch (IOException e) {
            return Pair.of("", 0l);
        }
    }
}

class DefaultDiskFreeSpaceCalculator implements IDiskFreeSpaceCalculator {

    @Override
    public Pair<String, Long> getFreeDiskSize(String path) {
        return Pair.of("", 0l);
    }
}