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
package org.apache.asterix.active;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ActiveJobNotificationHandler implements Runnable {
    public static final ActiveJobNotificationHandler INSTANCE = new ActiveJobNotificationHandler();
    public static final String ACTIVE_ENTITY_PROPERTY_NAME = "ActiveJob";
    private static final Logger LOGGER = Logger.getLogger(ActiveJobNotificationHandler.class.getName());
    private static final boolean DEBUG = false;
    private final LinkedBlockingQueue<ActiveEvent> eventInbox;
    private final Map<EntityId, IActiveEntityEventsListener> listeners;
    private final Map<JobId, EntityId> jobs2Entities;

    private ActiveJobNotificationHandler() {
        this.eventInbox = new LinkedBlockingQueue<>();
        this.jobs2Entities = new HashMap<>();
        this.listeners = new HashMap<>();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(ActiveJobNotificationHandler.class.getSimpleName());
        LOGGER.log(Level.INFO, "Started " + ActiveJobNotificationHandler.class.getSimpleName());
        while (!Thread.interrupted()) {
            try {
                ActiveEvent event = getEventInbox().take();
                EntityId entityId = jobs2Entities.get(event.getJobId());
                if (entityId != null) {
                    IActiveEntityEventsListener listener = listeners.get(entityId);
                    if (DEBUG) {
                        LOGGER.log(Level.WARNING, "Next event is of type " + event.getEventKind());
                        LOGGER.log(Level.WARNING, "Notifying the listener");
                    }
                    listener.notify(event);
                    if (event.getEventKind() == ActiveEvent.JOB_FINISH) {
                        if (DEBUG) {
                            LOGGER.log(Level.WARNING, "Removing the job");
                        }
                        jobs2Entities.remove(event.getJobId());
                        if (DEBUG) {
                            LOGGER.log(Level.WARNING, "Removing the listener since it is not active anymore");
                        }
                        listeners.remove(listener.getEntityId());
                    }
                } else {
                    LOGGER.log(Level.SEVERE, "Entity not found for received message for job " + event.getJobId());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error handling an active job event", e);
            }
        }
        LOGGER.log(Level.INFO, "Stopped " + ActiveJobNotificationHandler.class.getSimpleName());
    }

    public IActiveEntityEventsListener getActiveEntityListener(EntityId entityId) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "getActiveEntityListener(EntityId entityId) was called with entity " + entityId);
            IActiveEntityEventsListener listener = listeners.get(entityId);
            LOGGER.log(Level.WARNING, "Listener found: " + listener);
        }
        return listeners.get(entityId);
    }

    public EntityId getEntity(JobId jobId) {
        return jobs2Entities.get(jobId);
    }

    public void notifyJobCreation(JobId jobId, JobSpecification jobSpecification) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING,
                    "notifyJobCreation(JobId jobId, JobSpecification jobSpecification) was called with jobId = "
                            + jobId);
        }
        Object property = jobSpecification.getProperty(ACTIVE_ENTITY_PROPERTY_NAME);
        if (property == null || !(property instanceof EntityId)) {
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "Job was is not active. property found to be: " + property);
            }
            return;
        }
        EntityId entityId = (EntityId) property;
        monitorJob(jobId, entityId);
        if (DEBUG) {
            boolean found = jobs2Entities.get(jobId) != null;
            LOGGER.log(Level.WARNING, "Job was found to be: " + (found ? "Active" : "Inactive"));
        }
        IActiveEntityEventsListener listener = listeners.get(entityId);
        if (listener != null) {
            listener.notify(new ActiveEvent(jobId, ActiveEvent.JOB_CREATED, entityId, jobSpecification));
        }
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "Listener was notified" + jobId);
        }
    }

    public LinkedBlockingQueue<ActiveEvent> getEventInbox() {
        return eventInbox;
    }

    public synchronized IActiveEntityEventsListener[] getEventListeners() {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "getEventListeners() was called");
            LOGGER.log(Level.WARNING, "returning " + listeners.size() + " Listeners");
        }
        return listeners.values().toArray(new IActiveEntityEventsListener[listeners.size()]);
    }

    public synchronized void registerListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        if (DEBUG) {
            LOGGER.log(Level.WARNING,
                    "registerListener(IActiveEntityEventsListener listener) was called for the entity "
                            + listener.getEntityId());
        }
        if (listeners.containsKey(listener.getEntityId())) {
            throw new HyracksDataException(
                    "Active Entity Listener " + listener.getEntityId() + " is already registered");
        }
        listeners.put(listener.getEntityId(), listener);
    }

    public synchronized void monitorJob(JobId jobId, EntityId activeJob) {
        if (DEBUG) {
            LOGGER.log(Level.WARNING, "monitorJob(JobId jobId, ActiveJob activeJob) called with job id: " + jobId);
            boolean found = jobs2Entities.get(jobId) != null;
            LOGGER.log(Level.WARNING, "Job was found to be: " + (found ? "Active" : "Inactive"));
        }
        if (listeners.containsKey(activeJob)) {
            if (jobs2Entities.containsKey(jobId)) {
                LOGGER.severe("Job is already being monitored for job: " + jobId);
                return;
            }
            if (DEBUG) {
                LOGGER.log(Level.WARNING, "monitoring started for job id: " + jobId);
            }
        } else {
            LOGGER.severe("No listener was found for the entity: " + activeJob);
        }
        jobs2Entities.put(jobId, activeJob);
    }

    public synchronized void unregisterListener(IActiveEntityEventsListener listener) throws HyracksDataException {
        if (DEBUG) {
            LOGGER.log(Level.WARNING,
                    "unregisterListener(IActiveEntityEventsListener listener) was called for the entity "
                            + listener.getEntityId());
        }
        IActiveEntityEventsListener registeredListener = listeners.remove(listener.getEntityId());
        if (registeredListener == null) {
            throw new HyracksDataException(
                    "Active Entity Listener " + listener.getEntityId() + " hasn't been registered");
        }
    }
}
