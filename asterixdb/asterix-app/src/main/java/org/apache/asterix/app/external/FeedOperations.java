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
package org.apache.asterix.app.external;

import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedConnectionRequest;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedActivity;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMessageOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.file.JobSpecificationUtils;
import org.apache.asterix.lang.aql.statement.SubscribeFeedStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.feeds.LocationConstraint;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.job.listener.MultiTransactionJobletEventListenerFactory;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.asterix.translator.CompiledStatements;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
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
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningWithMessageConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.SplitOperatorDescriptor;
import org.json.JSONException;

/**
 * Provides helper method(s) for creating JobSpec for operations on a feed.
 */
public class FeedOperations {

    static final Logger LOGGER = Logger.getLogger(FeedOperations.class.getName());

    private static final ILangCompilationProvider compilationProvider = new AqlCompilationProvider();
    private static final DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory(null);

    private static Pair<JobSpecification, IAdapterFactory> buildFeedIntakeJobSpec(Feed feed,
            AqlMetadataProvider metadataProvider, FeedPolicyAccessor policyAccessor) throws Exception {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        spec.setFrameSize(FeedConstants.JobConstants.DEFAULT_FRAME_SIZE);
        IAdapterFactory adapterFactory;
        IOperatorDescriptor feedIngestor;
        AlgebricksPartitionConstraint ingesterPc;
        Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> t = metadataProvider
                .buildFeedIntakeRuntime(spec, feed, policyAccessor);
        feedIngestor = t.first;
        ingesterPc = t.second;
        adapterFactory = t.third;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedIngestor, ingesterPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, ingesterPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedIngestor, 0, nullSink, 0);
        spec.addRoot(nullSink);
        return Pair.of(spec, adapterFactory);
    }

    public static JobSpecification buildRemoveFeedStorageJob(Feed feed) throws Exception {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        AlgebricksAbsolutePartitionConstraint allCluster = ClusterStateManager.INSTANCE.getClusterLocations();
        Set<String> nodes = new TreeSet<>();
        for (String node : allCluster.getLocations()) {
            nodes.add(node);
        }
        AlgebricksAbsolutePartitionConstraint locations = new AlgebricksAbsolutePartitionConstraint(
                nodes.toArray(new String[nodes.size()]));
        FileSplit[] feedLogFileSplits = FeedUtils.splitsForAdapter(feed.getDataverseName(), feed.getFeedName(),
                locations);
        org.apache.hyracks.algebricks.common.utils.Pair<IFileSplitProvider, AlgebricksPartitionConstraint>
                splitsAndConstraint = StoragePathUtil
                .splitProviderAndPartitionConstraints(feedLogFileSplits);
        FileRemoveOperatorDescriptor frod = new FileRemoveOperatorDescriptor(spec, splitsAndConstraint.first, true);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, frod, splitsAndConstraint.second);
        spec.addRoot(frod);
        return spec;
    }

    private static JobSpecification getConnectionJob(AqlMetadataProvider metadataProvider,
            FeedConnection feedConnection, String[] locations)
            throws AlgebricksException, RemoteException, ACIDException, JSONException {
        PrintWriter writer = new PrintWriter(System.err, true);
        SessionConfig pc = new SessionConfig(writer, SessionConfig.OutputFormat.ADM);
        DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(feedConnection.getDataverseName()));
        FeedConnectionRequest fcr = new FeedConnectionRequest(FeedRuntimeType.INTAKE,
                feedConnection.getAppliedFunctions(), feedConnection.getDatasetName(), feedConnection.getPolicyName(),
                feedConnection.getFeedId());
        SubscribeFeedStatement subscribeStmt = new SubscribeFeedStatement(locations, fcr);
        subscribeStmt.initialize(metadataProvider.getMetadataTxnContext());
        List<Statement> statements = new ArrayList<>();
        statements.add(dataverseDecl);
        statements.add(subscribeStmt);
        IStatementExecutor translator = qtFactory.create(statements, pc, compilationProvider);
        // configure the metadata provider
        metadataProvider.getConfig().put(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS, "" + Boolean.TRUE);
        metadataProvider.getConfig().put(FeedActivity.FeedActivityDetails.FEED_POLICY_NAME,
                "" + subscribeStmt.getPolicy());
        metadataProvider.getConfig().put(FeedActivity.FeedActivityDetails.COLLECT_LOCATIONS,
                StringUtils.join(subscribeStmt.getLocations(), ','));

        CompiledStatements.CompiledSubscribeFeedStatement csfs = new CompiledStatements.CompiledSubscribeFeedStatement(
                subscribeStmt.getSubscriptionRequest(), subscribeStmt.getVarCounter());
        return translator.rewriteCompileQuery(metadataProvider, subscribeStmt.getQuery(), csfs);
    }

    private static JobSpecification combineIntakeCollectJobs(AqlMetadataProvider metadataProvider, Feed feed,
            JobSpecification intakeJob, List<JobSpecification> jobsList, List<FeedConnection> feedConnections)
            throws AsterixException {
        JobSpecification jobSpec = new JobSpecification(intakeJob.getFrameSize());

        // copy ingestor
        FeedIntakeOperatorDescriptor firstOp = (FeedIntakeOperatorDescriptor) intakeJob.getOperatorMap()
                .get(new OperatorDescriptorId(0));
        FeedIntakeOperatorDescriptor ingestionOp = new FeedIntakeOperatorDescriptor(jobSpec, feed,
                firstOp.getAdaptorFactory(), firstOp.getAdapterOutputType(), firstOp.getPolicyAccessor(),
                firstOp.getOutputRecordDescriptors()[0]);
        // create replicator
        SplitOperatorDescriptor replicateOp = new SplitOperatorDescriptor(jobSpec,
                ingestionOp.getOutputRecordDescriptors()[0], jobsList.size());
        jobSpec.connect(new OneToOneConnectorDescriptor(jobSpec), ingestionOp, 0, replicateOp, 0);
        String[] locationConstraints = ClusterStateManager.INSTANCE.getParticipantNodes()
                .toArray(new String[ClusterStateManager.INSTANCE.getParticipantNodes().size()]);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, ingestionOp, locationConstraints);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, replicateOp, locationConstraints);
        // Loop over the jobs to copy operators and connections
        Map<OperatorDescriptorId, OperatorDescriptorId> operatorIdMapping = new HashMap<>();
        Map<ConnectorDescriptorId, ConnectorDescriptorId> connectorIdMapping = new HashMap<>();
        Map<OperatorDescriptorId, List<LocationConstraint>> operatorLocations = new HashMap<>();
        Map<OperatorDescriptorId, Integer> operatorCounts = new HashMap<>();
        List<JobId> jobIds = new ArrayList<>();

        for (int iter1 = 0; iter1 < jobsList.size(); iter1++) {
            FeedConnection curFeedConnection = feedConnections.get(iter1);
            JobSpecification subJob = jobsList.get(iter1);
            operatorIdMapping.clear();
            Map<OperatorDescriptorId, IOperatorDescriptor> operatorsMap = subJob.getOperatorMap();

            FeedPolicyEntity feedPolicyEntity = FeedMetadataUtil.validateIfPolicyExists(
                    curFeedConnection.getDataverseName(), curFeedConnection.getPolicyName(),
                    metadataProvider.getMetadataTxnContext());

            for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operatorsMap.entrySet()) {
                IOperatorDescriptor opDesc = entry.getValue();
                OperatorDescriptorId oldId = opDesc.getOperatorId();
                OperatorDescriptorId opId;
                if (opDesc instanceof AsterixLSMTreeInsertDeleteOperatorDescriptor
                        && ((AsterixLSMTreeInsertDeleteOperatorDescriptor) opDesc).isPrimary()) {
                    String operandId = ((AsterixLSMTreeInsertDeleteOperatorDescriptor) opDesc).getIndexName();
                    FeedMetaOperatorDescriptor metaOp = new FeedMetaOperatorDescriptor(jobSpec,
                            new FeedConnectionId(ingestionOp.getEntityId(),
                                    feedConnections.get(iter1).getDatasetName()),
                            opDesc, feedPolicyEntity.getProperties(), FeedRuntimeType.STORE, false, operandId);
                    opId = metaOp.getOperatorId();
                    opDesc.setOperatorId(opId);
                } else if (opDesc instanceof FeedCollectOperatorDescriptor) {
                    continue;
                } else {
                    if (opDesc instanceof AlgebricksMetaOperatorDescriptor) {
                        AlgebricksMetaOperatorDescriptor algOp = (AlgebricksMetaOperatorDescriptor) opDesc;
                        for (IPushRuntimeFactory runtimeFactory : algOp.getPipeline().getRuntimeFactories()) {
                            if (runtimeFactory instanceof StreamSelectRuntimeFactory) {
                                ((StreamSelectRuntimeFactory) runtimeFactory).retainMissing(true, 0);
                            }
                        }
                    }
                    opId = jobSpec.createOperatorDescriptorId(opDesc);
                }
                operatorIdMapping.put(oldId, opId);
            }

            // copy connectors
            connectorIdMapping.clear();
            for (Entry<ConnectorDescriptorId, IConnectorDescriptor> entry : subJob.getConnectorMap().entrySet()) {
                IConnectorDescriptor connDesc = entry.getValue();
                ConnectorDescriptorId newConnId;
                if (connDesc instanceof MToNPartitioningConnectorDescriptor) {
                    MToNPartitioningConnectorDescriptor m2nConn = (MToNPartitioningConnectorDescriptor) connDesc;
                    if (entry.getKey().getId() == 0) {
                        continue;
                    }
                    connDesc = new MToNPartitioningWithMessageConnectorDescriptor(jobSpec,
                            m2nConn.getTuplePartitionComputerFactory());
                    newConnId = connDesc.getConnectorId();
                } else {
                    newConnId = jobSpec.createConnectorDescriptor(connDesc);
                }
                connectorIdMapping.put(entry.getKey(), newConnId);
            }

            // make connections between operators
            for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>,
                    Pair<IOperatorDescriptor, Integer>>> entry : subJob
                    .getConnectorOperatorMap().entrySet()) {
                ConnectorDescriptorId newId = connectorIdMapping.get(entry.getKey());
                IConnectorDescriptor connDesc = jobSpec.getConnectorMap().get(newId);
                Pair<IOperatorDescriptor, Integer> leftOp = entry.getValue().getLeft();
                Pair<IOperatorDescriptor, Integer> rightOp = entry.getValue().getRight();
                if (leftOp.getLeft() instanceof FeedCollectOperatorDescriptor) {
                    IOperatorDescriptor rightDesc = jobSpec.getOperatorMap().get(rightOp.getLeft().getOperatorId());
                    jobSpec.connect(new OneToOneConnectorDescriptor(jobSpec), replicateOp, iter1, rightDesc,
                            rightOp.getRight());
                } else {
                    IOperatorDescriptor leftOpDesc = jobSpec.getOperatorMap().get(leftOp.getLeft().getOperatorId());
                    IOperatorDescriptor rightOpDesc = jobSpec.getOperatorMap().get(rightOp.getLeft().getOperatorId());
                    jobSpec.connect(connDesc, leftOpDesc, leftOp.getRight(), rightOpDesc, rightOp.getRight());
                }
            }

            // prepare for setting partition constraints
            operatorLocations.clear();
            operatorCounts.clear();

            for (Constraint constraint : subJob.getUserConstraints()) {
                LValueConstraintExpression lexpr = constraint.getLValue();
                ConstraintExpression cexpr = constraint.getRValue();
                OperatorDescriptorId opId;
                switch (lexpr.getTag()) {
                    case PARTITION_COUNT:
                        opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                        if (opId.getId() == 0) {
                            continue;
                        }
                        operatorCounts.put(operatorIdMapping.get(opId), (int) ((ConstantExpression) cexpr).getValue());
                        break;
                    case PARTITION_LOCATION:
                        opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                        if (opId.getId() == 0) {
                            continue;
                        }
                        IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(operatorIdMapping.get(opId));
                        List<LocationConstraint> locations = operatorLocations.get(opDesc.getOperatorId());
                        if (locations == null) {
                            locations = new ArrayList<>();
                            operatorLocations.put(opDesc.getOperatorId(), locations);
                        }
                        String location = (String) ((ConstantExpression) cexpr).getValue();
                        LocationConstraint lc = new LocationConstraint(location,
                                ((PartitionLocationExpression) lexpr).getPartition());
                        locations.add(lc);
                        break;
                    default:
                        break;
                }
            }

            // set absolute location constraints
            for (Entry<OperatorDescriptorId, List<LocationConstraint>> entry : operatorLocations.entrySet()) {
                IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(entry.getKey());
                // why do we need to sort?
                Collections.sort(entry.getValue(), (LocationConstraint o1, LocationConstraint o2) -> {
                    return o1.partition - o2.partition;
                });
                String[] locations = new String[entry.getValue().size()];
                for (int j = 0; j < locations.length; ++j) {
                    locations[j] = entry.getValue().get(j).location;
                }
                PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, opDesc, locations);
            }

            // set count constraints
            for (Entry<OperatorDescriptorId, Integer> entry : operatorCounts.entrySet()) {
                IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(entry.getKey());
                if (!operatorLocations.keySet().contains(entry.getKey())) {
                    PartitionConstraintHelper.addPartitionCountConstraint(jobSpec, opDesc, entry.getValue());
                }
            }
            // roots
            for (OperatorDescriptorId root : subJob.getRoots()) {
                jobSpec.addRoot(jobSpec.getOperatorMap().get(operatorIdMapping.get(root)));
            }
            jobIds.add(((JobEventListenerFactory) subJob.getJobletEventListenerFactory()).getJobId());
        }

        // jobEventListenerFactory
        jobSpec.setJobletEventListenerFactory(new MultiTransactionJobletEventListenerFactory(jobIds, true));
        // useConnectorSchedulingPolicy
        jobSpec.setUseConnectorPolicyForScheduling(jobsList.get(0).isUseConnectorPolicyForScheduling());
        // connectorAssignmentPolicy
        jobSpec.setConnectorPolicyAssignmentPolicy(jobsList.get(0).getConnectorPolicyAssignmentPolicy());
        return jobSpec;
    }

    public static JobSpecification buildStartFeedJob(AqlMetadataProvider metadataProvider, Feed feed,
            List<FeedConnection> feedConnections) throws Exception {
        JobSpecification feedJob;
        String[] ingestionLocations;
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(new HashMap<>());
        // TODO: Change the default Datasource to use all possible partitions
        Pair<JobSpecification, IAdapterFactory> intakeInfo = buildFeedIntakeJobSpec(feed, metadataProvider, fpa);
        //TODO: Add feed policy accessor
        List<JobSpecification> jobsList = new ArrayList<>();
        // Construct the ingestion Job
        JobSpecification intakeJob = intakeInfo.getLeft();
        IAdapterFactory ingestionAdaptorFactory = intakeInfo.getRight();
        ingestionLocations = ingestionAdaptorFactory.getPartitionConstraint().getLocations();
        // Add connection job
        for (FeedConnection feedConnection : feedConnections) {
            JobSpecification connectionJob = getConnectionJob(metadataProvider, feedConnection, ingestionLocations);
            jobsList.add(connectionJob);
        }
        feedJob = combineIntakeCollectJobs(metadataProvider, feed, intakeJob, jobsList, feedConnections);
        return feedJob;
    }

    private static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildSendFeedMessageRuntime(
            JobSpecification jobSpec, EntityId feedId, String runtimeType, byte messageType,
            Collection<String> locations) throws AlgebricksException {
        AlgebricksPartitionConstraint partitionConstraint = new AlgebricksAbsolutePartitionConstraint(
                locations.toArray(new String[] {}));
        FeedMessageOperatorDescriptor feedMessenger = new FeedMessageOperatorDescriptor(jobSpec, feedId, runtimeType,
                messageType);
        return Pair.of(feedMessenger, partitionConstraint);
    }

    private static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDisconnectFeedMessengerRuntime(
            JobSpecification jobSpec, EntityId feedId, List<String> locations) throws AlgebricksException {
        return buildSendFeedMessageRuntime(jobSpec, feedId, FeedRuntimeType.INTAKE.toString(),
                ActiveManagerMessage.STOP_ACTIVITY, locations);
    }

    public static JobSpecification buildStopFeedJob(EntityId feedId, List<String> intakeNodeLocations)
            throws AlgebricksException {
        JobSpecification jobSpec = JobSpecificationUtils.createJobSpecification();
        AlgebricksPartitionConstraint messengerPc;
        IOperatorDescriptor feedMessenger;

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = buildDisconnectFeedMessengerRuntime(jobSpec,
                feedId, intakeNodeLocations);

        feedMessenger = p.getLeft();
        messengerPc = p.getRight();

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, feedMessenger, messengerPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(jobSpec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(jobSpec, nullSink, messengerPc);
        jobSpec.connect(new OneToOneConnectorDescriptor(jobSpec), feedMessenger, 0, nullSink, 0);
        jobSpec.addRoot(nullSink);

        return jobSpec;
    }

    public static void SendStopMessageToNode(EntityId feedId, String intakeNodeLocation, Integer ioDeviceIdx)
            throws Exception {
        ActiveManagerMessage stopFeedMessage = new ActiveManagerMessage(ActiveManagerMessage.STOP_ACTIVITY, "SRC",
                new ActiveRuntimeId(feedId, FeedUtils.FeedRuntimeType.INTAKE.toString(), ioDeviceIdx));
        SendActiveMessage(stopFeedMessage, intakeNodeLocation);
    }

    private static void SendActiveMessage(ActiveManagerMessage activeManagerMessage, String nodeId) throws Exception {
        ICCMessageBroker messageBroker = (ICCMessageBroker) AsterixAppContextInfo.INSTANCE.getCCApplicationContext()
                .getMessageBroker();
        messageBroker.sendApplicationMessageToNC(activeManagerMessage, nodeId);
    }
}