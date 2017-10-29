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

package org.apache.asterix.lang.common.statement;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.external.feed.watch.StatsSubscriber;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class StartFeedStatement implements Statement {

    public static final String WAIT_FOR_COMPLETION = "wait-for-completion-feed";
    private Identifier dataverseName;
    private Identifier feedName;

    public StartFeedStatement(Pair<Identifier, Identifier> feedNameComp) {
        dataverseName = feedNameComp.first;
        feedName = feedNameComp.second;
    }

    @Override
    public byte getKind() {
        return Kind.START_FEED;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getFeedName() {
        return feedName;
    }

}
