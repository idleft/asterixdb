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
package org.apache.asterix.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.LSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.operators.FeedCallableOperatorDescriptor;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorNodePushable;
import org.apache.asterix.external.operators.DeployedJobPartitionHolderDescriptor;
import org.apache.asterix.external.operators.FeedPipelineSinkDescriptor;
import org.apache.asterix.external.operators.StoragePartitionHolderDescriptor;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.UpsertStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.feeds.LocationConstraint;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.constraints.expressions.ConstantExpression;
import org.apache.hyracks.api.constraints.expressions.ConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.LValueConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionCountExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionLocationExpression;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.partition.RandomPartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.RoundrobinPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

import static org.apache.asterix.external.util.FeedConstants.FEED_INTAKE_PARTITION_HOLDER;

/**
 * Provides helper method(s) for creating JobSpec for operations on a feed.
 */
public class FeedOperations {

    public static final String FEED_DATAFLOW_INTERMEIDATE_VAL_PREFIX = "val";

    private FeedOperations() {
    }

    private static Set<String> getStgNodes(MetadataProvider metadataProvider, List<FeedConnection> feedConns)
            throws AlgebricksException {
        Set<String> stgNodes = new HashSet<>();
        for (FeedConnection feedConn : feedConns) {
            Dataset ds = metadataProvider.findDataset(feedConn.getDataverseName(), feedConn.getDatasetName());
            stgNodes.addAll(metadataProvider.findNodes(ds.getNodeGroupName()));
        }
        return stgNodes;
    }

    private static Pair<JobSpecification, IAdapterFactory> buildFeedIntakeJobSpec(Feed feed,
            MetadataProvider metadataProvider, FeedPolicyAccessor policyAccessor, List<FeedConnection> feedConns,
            String[] collectLocations) throws Exception {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        spec.setFrameSize(metadataProvider.getApplicationContext().getCompilerProperties().getFrameSize());
        IAdapterFactory adapterFactory;
        IOperatorDescriptor intakeOperator;
        AlgebricksPartitionConstraint ingesterPc;
        Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> t =
                metadataProvider.buildFeedIntakeRuntime(spec, feed, policyAccessor, collectLocations.length,
                        getStgNodes(metadataProvider, feedConns));
        intakeOperator = t.first;
        ingesterPc = t.second;
        adapterFactory = t.third;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, intakeOperator, ingesterPc);
        MToNPartitioningConnectorDescriptor randomPartitioner =
                new MToNPartitioningConnectorDescriptor(spec, new RoundrobinPartitionComputerFactory());
        int workerNum = Integer.valueOf(feed.getConfiguration().getOrDefault(FeedConstants.WORKER_NUM, "1"));
        int intakePoolSize = Integer.valueOf(feed.getConfiguration().getOrDefault(FeedConstants.INTAKE_POOL_SIZE, "1"));
        DeployedJobPartitionHolderDescriptor intakePartitionHolder = new DeployedJobPartitionHolderDescriptor(spec,
                intakePoolSize, feed.getFeedId(), FEED_INTAKE_PARTITION_HOLDER, workerNum);
        spec.connect(randomPartitioner, intakeOperator, 0, intakePartitionHolder, 0);
        //TODO: the partition constraint of intake partition holder is set to cluster location now.
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, intakePartitionHolder, collectLocations);
        spec.addRoot(intakePartitionHolder);
        return Pair.of(spec, adapterFactory);
    }

    public static JobSpecification buildRemoveFeedStorageJob(MetadataProvider metadataProvider, Feed feed)
            throws AsterixException {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        JobSpecification spec = RuntimeUtils.createJobSpecification(appCtx);
        IClusterStateManager csm = appCtx.getClusterStateManager();
        AlgebricksAbsolutePartitionConstraint allCluster = csm.getClusterLocations();
        Set<String> nodes = new TreeSet<>();
        for (String node : allCluster.getLocations()) {
            nodes.add(node);
        }
        AlgebricksAbsolutePartitionConstraint locations =
                new AlgebricksAbsolutePartitionConstraint(nodes.toArray(new String[nodes.size()]));
        FileSplit[] feedLogFileSplits =
                FeedUtils.splitsForAdapter(appCtx, feed.getDataverseName(), feed.getFeedName(), locations);
        org.apache.hyracks.algebricks.common.utils.Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spC =
                StoragePathUtil.splitProviderAndPartitionConstraints(feedLogFileSplits);
        FileRemoveOperatorDescriptor frod = new FileRemoveOperatorDescriptor(spec, spC.first, true);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, frod, spC.second);
        spec.addRoot(frod);
        return spec;
    }

    private static List<Expression> addArgs(Object... args) {
        List<Expression> argExprs = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof Integer) {
                argExprs.add(new LiteralExpr(new IntegerLiteral((Integer) arg)));
            } else if (arg instanceof String) {
                argExprs.add(new LiteralExpr(new StringLiteral((String) arg)));
            } else if (arg instanceof VariableExpr) {
                argExprs.add((VariableExpr) arg);
            }
        }
        return argExprs;
    }

    private static Query makeConnectionQuery(FeedConnection feedConnection) throws AlgebricksException {
        // Construct from clause
        VarIdentifier fromVarId = SqlppVariableUtil.toInternalVariableIdentifier(feedConnection.getFeedName());
        VariableExpr fromTermLeftExpr = new VariableExpr(fromVarId);
        // TODO: remove target feedid from args list (xikui)
        // TODO: Get rid of this INTAKE
        List<Expression> exprList = addArgs(feedConnection.getDataverseName(),
                feedConnection.getFeedId().getEntityName(), feedConnection.getFeedId().getEntityName(),
                FeedRuntimeType.INTAKE.toString(), feedConnection.getDatasetName(), feedConnection.getOutputType());
        CallExpr datasrouceCallFunction = new CallExpr(new FunctionSignature(BuiltinFunctions.FEED_COLLECT), exprList);
        FromTerm fromterm = new FromTerm(datasrouceCallFunction, fromTermLeftExpr, null, null);
        FromClause fromClause = new FromClause(Arrays.asList(fromterm));
        WhereClause whereClause = null;
        if (feedConnection.getWhereClauseBody().length() != 0) {
            String whereClauseExpr = feedConnection.getWhereClauseBody() + ";";
            IParserFactory sqlppParserFactory = new SqlppParserFactory();
            IParser sqlppParser = sqlppParserFactory.createParser(whereClauseExpr);
            List<Statement> stmts = sqlppParser.parse();
            if (stmts.size() != 1) {
                throw new CompilationException("Exceptions happened in processing where clause.");
            }
            Query whereClauseQuery = (Query) stmts.get(0);
            whereClause = new WhereClause(whereClauseQuery.getBody());
        }

        // TODO: This can be the place to add select predicate for ingestion
        // Attaching functions
        int varIdx = 1;
        VariableExpr previousVarExpr = fromTermLeftExpr;
        ArrayList<LetClause> letClauses = new ArrayList<>();
        for (FunctionSignature funcSig : feedConnection.getAppliedFunctions()) {
            VarIdentifier intermediateVar = SqlppVariableUtil
                    .toInternalVariableIdentifier(FEED_DATAFLOW_INTERMEIDATE_VAL_PREFIX + String.valueOf(varIdx));
            VariableExpr intermediateVarExpr = new VariableExpr(intermediateVar);
            CallExpr functionCallExpr = new CallExpr(funcSig, addArgs(previousVarExpr));
            previousVarExpr = intermediateVarExpr;
            LetClause letClause = new LetClause(intermediateVarExpr, functionCallExpr);
            letClauses.add(letClause);
            varIdx++;
        }
        // Constructing select clause
        SelectElement selectElement = new SelectElement(previousVarExpr);
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, letClauses, whereClause, null, null, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        SelectExpression body = new SelectExpression(null, selectSetOperation, null, null, true);
        Query query = new Query(false, true, body, 0);
        return query;
    }

    private static JobSpecification getConnectionJob(MetadataProvider metadataProvider, FeedConnection feedConn,
            IStatementExecutor statementExecutor, IHyracksClientConnection hcc, Boolean insertFeed)
            throws AlgebricksException, ACIDException {
        metadataProvider.getConfig().put(FeedActivityDetails.FEED_POLICY_NAME, feedConn.getPolicyName());
        metadataProvider.setWriteTransaction(true);
        Query feedConnQuery = makeConnectionQuery(feedConn);
        InsertStatement stmt;
        if (insertFeed) {
            stmt = new InsertStatement(new Identifier(feedConn.getDataverseName()),
                    new Identifier(feedConn.getDatasetName()), feedConnQuery, -1, null, null);
        } else {
            stmt = new UpsertStatement(new Identifier(feedConn.getDataverseName()),
                    new Identifier(feedConn.getDatasetName()), feedConnQuery, -1, null, null);
        }
        return statementExecutor.rewriteCompileInsertUpsert(hcc, metadataProvider, stmt, null, null);
    }

    private static IStatementExecutor getSQLPPTranslator(MetadataProvider metadataProvider,
            SessionOutput sessionOutput) {
        List<Statement> stmts = new ArrayList<>();
        DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory();
        IStatementExecutor translator = qtFactory.create(metadataProvider.getApplicationContext(), stmts, sessionOutput,
                new SqlppCompilationProvider(), new StorageComponentProvider());
        return translator;
    }

    public static Pair<JobSpecification, AlgebricksAbsolutePartitionConstraint> buildStartFeedJob(
            MetadataProvider metadataProvider, Feed feed, List<FeedConnection> feedConns, String[] collectLocations)
            throws Exception {
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(new HashMap<>());
        Pair<JobSpecification, IAdapterFactory> intakeInfo =
                buildFeedIntakeJobSpec(feed, metadataProvider, fpa, feedConns, collectLocations);
        // Construct the ingestion Job
        JobSpecification intakeJob = intakeInfo.getLeft();
        return Pair.of(intakeJob, intakeInfo.getRight().getPartitionConstraint());
    }

    public static JobSpecification buildFeedConnectionJob(MetadataProvider metadataProvider,
            List<FeedConnection> feedConnections, IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            Feed feed, String[] collectLocations) throws Exception {
        // Add metadata configs
        metadataProvider.getConfig().put(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS, Boolean.TRUE.toString());
        metadataProvider.getConfig().put(FeedActivityDetails.COLLECT_LOCATIONS,
                StringUtils.join(collectLocations, ','));
        // TODO: Once we deprecated AQL, this extra queryTranslator can be removed.
        IStatementExecutor translator =
                getSQLPPTranslator(metadataProvider, ((QueryTranslator) statementExecutor).getSessionOutput());
        // Add connection job
        Boolean insertFeed = ExternalDataUtils.isInsertFeed(feed.getConfiguration());
        JobSpecification connJob =
                getConnectionJob(metadataProvider, feedConnections.get(0), translator, hcc, insertFeed);
        return connJob;
    }

    private static void findOpsAfterOp(JobSpecification connJob, IOperatorDescriptor op,
            List<IOperatorDescriptor> afterOpOps) {
        List<IConnectorDescriptor> afterConns = connJob.getOperatorOutputMap().get(op.getOperatorId());
        if (afterConns == null) {
            return;
        }
        for (IConnectorDescriptor branch : afterConns) {
            IOperatorDescriptor branchTopOp =
                    connJob.getConnectorOperatorMap().get(branch.getConnectorId()).getRight().getKey();
            afterOpOps.add(branchTopOp);
            findOpsAfterOp(connJob, branchTopOp, afterOpOps);
        }
    }

    public static Pair<JobSpecification, JobSpecification> decoupleStorageJob(JobSpecification connJob, Feed feed) {
        JobSpecification pipeLineJob = new JobSpecification();
        JobSpecification storageJob = new JobSpecification();
        // TODO: can we combine these two?
        Map<OperatorDescriptorId, OperatorDescriptorId> pipeLineOpMapping = new HashMap<>();
        Map<OperatorDescriptorId, OperatorDescriptorId> storageOpMapping = new HashMap<>();
        // constraints of old job
        Map<OperatorDescriptorId, List<LocationConstraint>> operatorLocations = new HashMap<>();
        Map<OperatorDescriptorId, Integer> operatorCounts = new HashMap<>();
        List<IOperatorDescriptor> stgOps = new ArrayList<>();
        IOperatorDescriptor stgRoot = null;
        IOperatorDescriptor pstgOp = null;

        // inject Pipeline sink and StoragePH into jobs
        // create the sphd in place to maek the output record desc right
        int storagePoolSize = Integer.valueOf(feed.getConfiguration().getOrDefault(FeedConstants.STG_POOL_SIZE, "1"));
        StoragePartitionHolderDescriptor sphd = null;
        FeedPipelineSinkDescriptor fpsd = new FeedPipelineSinkDescriptor(pipeLineJob, feed.getFeedId(),
                FeedConstants.FEED_STORAGE_PARTITION_HOLDER);

        // find the break point
        for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> entry : connJob.getOperatorMap().entrySet()) {
            if (entry.getValue() instanceof LSMTreeInsertDeleteOperatorDescriptor) {
                pstgOp = entry.getValue();
                break;
            }
        }
        stgOps.add(pstgOp);
        findOpsAfterOp(connJob, pstgOp, stgOps);
        // copy operators
        for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> entry : connJob.getOperatorMap().entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            OperatorDescriptorId oldOpId = entry.getKey();
            OperatorDescriptorId newOpId;
            if (stgOps.contains(opDesc)) {
                if (connJob.getRoots().contains(oldOpId)) {
                    stgRoot = opDesc;
                }
                // including primary and secondary
                newOpId = storageJob.createOperatorDescriptorId(opDesc);
                storageOpMapping.put(oldOpId, newOpId);
            } else {
                newOpId = pipeLineJob.createOperatorDescriptorId(opDesc);
                pipeLineOpMapping.put(oldOpId, newOpId);
            }
        }

        // make connections
        for (Map.Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : connJob
                .getConnectorOperatorMap().entrySet()) {
            Pair<IOperatorDescriptor, Integer> leftOp = entry.getValue().getLeft();
            Pair<IOperatorDescriptor, Integer> rightOp = entry.getValue().getRight();
            IOperatorDescriptor leftOpDesc = leftOp.getLeft();
            IOperatorDescriptor rightOpDesc = rightOp.getLeft();
            IConnectorDescriptor connDesc = connJob.getConnectorMap().get(entry.getKey());

            if (rightOpDesc instanceof LSMTreeInsertDeleteOperatorDescriptor
                    && ((LSMTreeInsertDeleteOperatorDescriptor) rightOpDesc).isPrimary()) {
                // plug in pipeline sink for pipeline job
                pipeLineJob.createConnectorDescriptor(connDesc);
                pipeLineJob.connect(connDesc, leftOpDesc, leftOp.getValue(), fpsd, 0);
                // plugin partition holder for storage job
                sphd = new StoragePartitionHolderDescriptor(storageJob, storagePoolSize, feed.getFeedId(),
                        FeedConstants.FEED_STORAGE_PARTITION_HOLDER, leftOpDesc.getOutputRecordDescriptors()[0]);
                storageJob.connect(new OneToOneConnectorDescriptor(storageJob), sphd, 0, rightOpDesc,
                        rightOp.getValue());
            } else {
                // for other connections that's not on the boundary
                if (stgOps.contains(leftOp.getKey())) {
                    // falls into stg job
                    storageJob.createConnectorDescriptor(connDesc);
                    storageJob.connect(connDesc, leftOpDesc, leftOp.getValue(), rightOpDesc, rightOp.getValue());
                } else {
                    // falls into pipeline job
                    pipeLineJob.createConnectorDescriptor(connDesc);
                    pipeLineJob.connect(connDesc, leftOpDesc, leftOp.getValue(), rightOpDesc, rightOp.getValue());
                }
            }
        }

        // update location constraints
        for (Constraint constraint : connJob.getUserConstraints()) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            OperatorDescriptorId opId;
            switch (lexpr.getTag()) {
                case PARTITION_COUNT:
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    operatorCounts.put(opId, (int) ((ConstantExpression) cexpr).getValue());
                    break;
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                    List<LocationConstraint> locations = operatorLocations.get(opId);
                    if (locations == null) {
                        locations = new ArrayList<>();
                        operatorLocations.put(opId, locations);
                    }
                    String location = (String) ((ConstantExpression) cexpr).getValue();
                    LocationConstraint lc =
                            new LocationConstraint(location, ((PartitionLocationExpression) lexpr).getPartition());
                    locations.add(lc);
                    break;
                default:
                    break;
            }
        }

        // set absolute location constraints
        for (Map.Entry<OperatorDescriptorId, List<LocationConstraint>> entry : operatorLocations.entrySet()) {
            String[] locations = new String[entry.getValue().size()];
            IOperatorDescriptor opDesc;
            // This sort is needed to make sure the partitions are consistent
            Collections.sort(entry.getValue(), Comparator.comparingInt((LocationConstraint o) -> o.partition));
            for (int j = 0; j < locations.length; ++j) {
                locations[j] = entry.getValue().get(j).location;
            }
            if (pipeLineOpMapping.containsKey(entry.getKey())) {
                opDesc = pipeLineJob.getOperatorMap().get(pipeLineOpMapping.get(entry.getKey()));
                PartitionConstraintHelper.addAbsoluteLocationConstraint(pipeLineJob, opDesc, locations);
            } else {
                opDesc = storageJob.getOperatorMap().get(storageOpMapping.get(entry.getKey()));
                PartitionConstraintHelper.addAbsoluteLocationConstraint(storageJob, opDesc, locations);
            }
            if (opDesc instanceof LSMTreeInsertDeleteOperatorDescriptor) {
                PartitionConstraintHelper.addAbsoluteLocationConstraint(pipeLineJob, fpsd, locations);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(storageJob, sphd, locations);
            }
        }

        // set count constraints
        for (Map.Entry<OperatorDescriptorId, Integer> entry : operatorCounts.entrySet()) {
            IOperatorDescriptor opDesc;
            if (pipeLineOpMapping.containsKey(entry.getKey())) {
                opDesc = pipeLineJob.getOperatorMap().get(pipeLineOpMapping.get(entry.getKey()));
                PartitionConstraintHelper.addPartitionCountConstraint(pipeLineJob, opDesc, entry.getValue());
            } else {
                opDesc = storageJob.getOperatorMap().get(storageOpMapping.get(entry.getKey()));
                PartitionConstraintHelper.addPartitionCountConstraint(storageJob, opDesc, entry.getValue());
            }
            if (opDesc instanceof LSMTreeInsertDeleteOperatorDescriptor) {
                PartitionConstraintHelper.addPartitionCountConstraint(pipeLineJob, fpsd, entry.getValue());
                PartitionConstraintHelper.addPartitionCountConstraint(storageJob, sphd, entry.getValue());
            }
        }

        // misc
        pipeLineJob.addRoot(fpsd);
        storageJob.addRoot(stgRoot);

        pipeLineJob.setJobletEventListenerFactory(connJob.getJobletEventListenerFactory());
        storageJob.setJobletEventListenerFactory(connJob.getJobletEventListenerFactory());

        pipeLineJob.setConnectorPolicyAssignmentPolicy(connJob.getConnectorPolicyAssignmentPolicy());
        storageJob.setConnectorPolicyAssignmentPolicy(connJob.getConnectorPolicyAssignmentPolicy());

        pipeLineJob.setUseConnectorPolicyForScheduling(connJob.isUseConnectorPolicyForScheduling());
        storageJob.setUseConnectorPolicyForScheduling(connJob.isUseConnectorPolicyForScheduling());

        return Pair.of(pipeLineJob, storageJob);
    }

    private static void SendActiveMessage(ICcApplicationContext appCtx, ActiveManagerMessage activeManagerMessage,
            String nodeId) throws Exception {
        ICCMessageBroker messageBroker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        messageBroker.sendApplicationMessageToNC(activeManagerMessage, nodeId);
    }
}