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
package org.apache.hyracks.algebricks.core.algebra.properties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint.PartitionConstraintType;

public class DefaultNodeGroupDomain implements INodeDomain {

    private List<String> nodes = new ArrayList<>();

    public DefaultNodeGroupDomain(List<String> nodes) {
        this.nodes.addAll(nodes);
        Collections.sort(nodes);
    }

    public DefaultNodeGroupDomain(DefaultNodeGroupDomain domain) {
        this.nodes.addAll(domain.nodes);
        Collections.sort(nodes);
    }

    public DefaultNodeGroupDomain(AlgebricksPartitionConstraint clusterLocations) {
        if (clusterLocations.getPartitionConstraintType() == PartitionConstraintType.ABSOLUTE) {
            AlgebricksAbsolutePartitionConstraint absPc = (AlgebricksAbsolutePartitionConstraint) clusterLocations;
            nodes.addAll(Arrays.asList(absPc.getLocations()));
            Collections.sort(nodes);
        } else {
            throw new IllegalStateException("A node domain can only take absolute location constraints.");
        }
    }

    @Override
    public boolean sameAs(INodeDomain domain) {
        if (!(domain instanceof DefaultNodeGroupDomain)) {
            return false;
        }
        DefaultNodeGroupDomain nodeDomain = (DefaultNodeGroupDomain) domain;
        Collections.sort(nodes);
        Collections.sort(nodeDomain.nodes);
        return nodes.equals(nodeDomain.nodes);
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    @Override
    public Integer cardinality() {
        return nodes.size();
    }

    public String[] getNodes() {
        return nodes.toArray(new String[0]);
    }
}
