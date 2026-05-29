/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.xinfra.lab.raft;

import io.github.xinfra.lab.raft.proto.Eraftpb;
import io.github.xinfra.lab.raft.tracker.Progress;
import io.github.xinfra.lab.raft.tracker.ProgressTracker;

import java.util.HashMap;
import java.util.Map;

/**
 * Status contains information about this Raft peer and its view of the system.
 * The Progress is only populated on the leader.
 */
public class Status {
    public BasicStatus basicStatus;
    public ProgressTracker.Config config;
    public Map<Long, Progress> progress;

    public static class BasicStatus {
        public long id;
        public Eraftpb.HardState hardState;
        public SoftState softState;
        public long applied;
        public long leadTransferee;
    }

    public static BasicStatus getBasicStatus(Raft r) {
        BasicStatus s = new BasicStatus();
        s.id = r.id;
        s.leadTransferee = r.leadTransferee;
        s.hardState = r.hardState();
        s.softState = r.softState();
        s.applied = r.raftLog.applied;
        return s;
    }

    public static Status getStatus(Raft r) {
        Status s = new Status();
        s.basicStatus = getBasicStatus(r);
        if (s.basicStatus.softState.raftState == RaftStateType.StateLeader) {
            s.progress = new HashMap<>();
            r.trk.visit((id, pr) -> {
                Progress p = pr.clone();
                s.progress.put(id, p);
            });
        }
        s.config = r.trk.getConfig().clone();
        return s;
    }

    /**
     * Returns a JSON representation of this Status, matching the Go
     * MarshalJSON implementation in etcd-raft's status.go.
     */
    public String toJson() {
        StringBuilder j = new StringBuilder();
        j.append(String.format("{\"id\":\"%x\",\"term\":%d,\"vote\":\"%x\",\"commit\":%d,\"lead\":\"%x\",\"raftState\":\"%s\",\"applied\":%d,\"progress\":{",
                basicStatus.id,
                basicStatus.hardState.getTerm(),
                basicStatus.hardState.getVote(),
                basicStatus.hardState.getCommit(),
                basicStatus.softState.lead,
                basicStatus.softState.raftState,
                basicStatus.applied));

        if (progress == null || progress.isEmpty()) {
            j.append("},");
        } else {
            boolean first = true;
            for (Map.Entry<Long, Progress> entry : progress.entrySet()) {
                if (!first) j.append(',');
                first = false;
                Progress v = entry.getValue();
                j.append(String.format("\"%x\":{\"match\":%d,\"next\":%d,\"state\":\"%s\"}",
                        entry.getKey(), v.getMatch(), v.getNext(), v.getState()));
            }
            j.append("},");
        }

        j.append(String.format("\"leadtransferee\":\"%x\"}", basicStatus.leadTransferee));
        return j.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
