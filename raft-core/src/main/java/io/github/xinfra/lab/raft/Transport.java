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

/**
 * Pluggable network transport for raft messages. {@code raft-core} ships
 * <i>only</i> this interface — the wire format, framing, TLS, connection
 * pooling, and reliability are all the implementation's concern, not raft's.
 *
 * <p>Reference impls in sibling modules:
 * <ul>
 *   <li>{@code raft-transport-grpc} — gRPC unary RPC, bidi-stream for
 *   snapshots; suitable as a starting point for production deployments.</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 *
 * <pre>{@code
 *   Transport t = new GrpcTransport(...);
 *   t.setReceiver(msg -> node.step(msg));
 *   t.addPeer(2L, "peer-2:8080");
 *   t.addPeer(3L, "peer-3:8080");
 *   t.start();
 *   // ... while node is running, drain Ready and:
 *   for (Message m : ready.messages) t.send(m.getTo(), m);
 *   // ... shutdown:
 *   t.close();
 * }</pre>
 *
 * <h2>Required guarantees (the implementation is responsible)</h2>
 * <ul>
 *   <li><b>Best-effort delivery, not exactly-once.</b> Raft tolerates
 *   message loss and duplication. The transport may drop on a full send
 *   buffer — but should call {@link Node#reportUnreachable(long)} so the
 *   leader can pause replication.</li>
 *
 *   <li><b>Ordering not required.</b> Raft attaches its own term/index;
 *   reordering is fine. Implementations may use unordered transports.</li>
 *
 *   <li><b>Snapshot reporting.</b> When {@link Node#reportSnapshot} is
 *   called by the host upon snapshot send completion / failure, the
 *   transport (or the host) must invoke it; otherwise the leader stays
 *   in {@code StateSnapshot} until heartbeat timeout.</li>
 *
 *   <li><b>Loopback elision.</b> Messages whose {@code to} equals the
 *   local node id should be routed back through {@link MessageReceiver}
 *   directly — never put on the wire.</li>
 * </ul>
 */
public interface Transport extends AutoCloseable {

    /**
     * Bind the receiver that handles inbound messages. Must be called
     * before {@link #start()}. Typically the host wires this to
     * {@code rawNode.step} (single-thread) or {@code node.step}
     * (DefaultNode).
     */
    void setReceiver(MessageReceiver receiver);

    /**
     * Register a peer's network endpoint. The address format is
     * implementation-defined (gRPC uses {@code host:port}). Calling with
     * an existing peerId replaces the address; the implementation may
     * close the old connection lazily.
     */
    void addPeer(long peerId, String address);

    /**
     * Remove a peer. The implementation closes any pending connections
     * and discards queued messages.
     */
    void removePeer(long peerId);

    /**
     * Send a raft message to {@code peerId}. Non-blocking, best-effort.
     * The transport SHOULD return quickly even when the peer is
     * unreachable; reporting unreachability happens via
     * {@link Node#reportUnreachable}.
     */
    void send(long peerId, Eraftpb.Message msg);

    /**
     * Start the transport (e.g. bind the server socket, accept inbound
     * connections). After {@code start()} returns, inbound messages will
     * be dispatched to the receiver.
     */
    void start();

    /**
     * Shut down the transport. Closes all connections and stops the
     * server. Safe to call multiple times. Implementations should
     * complete any in-flight sends with their best effort but are not
     * required to deliver after {@code close()}.
     */
    @Override
    void close();

    /** Callback fired by the transport for each inbound raft message. */
    @FunctionalInterface
    interface MessageReceiver {
        /**
         * Handle one inbound message. Implementations must return quickly;
         * the call is on a transport thread and blocking it stalls the
         * whole pipeline.
         */
        void receive(Eraftpb.Message msg);
    }
}
