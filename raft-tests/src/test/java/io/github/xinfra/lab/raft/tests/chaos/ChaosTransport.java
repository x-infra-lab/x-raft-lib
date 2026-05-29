/*
 * Copyright 2024-2026 The x-raft-lib Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.xinfra.lab.raft.tests.chaos;

import io.github.xinfra.lab.raft.Transport;
import io.github.xinfra.lab.raft.proto.Eraftpb;

/**
 * A fault-injecting {@link Transport} decorator. It wraps a real transport
 * (e.g. {@code GrpcTransport}) and consults a shared {@link ChaosController}
 * to drop messages on the way out and on the way in, simulating partitions,
 * isolated nodes, and lossy links — without changing raft-core or the real
 * transport at all.
 *
 * <p>Every node in a chaos test wraps its own transport with one of these,
 * all sharing a single {@link ChaosController}. Dropping is applied on both
 * the outbound {@link #send} path (link {@code localId -> peer}) and the
 * inbound receive path (link {@code msg.from -> localId}); doing both makes a
 * partition symmetric even if one side's send slips through a race.
 *
 * <p>This decorator deliberately models only loss/partition, the faults raft
 * is designed to tolerate. It never corrupts or reorders payloads.
 */
public final class ChaosTransport implements Transport {

    private final long localId;
    private final Transport delegate;
    private final ChaosController controller;

    public ChaosTransport(long localId, Transport delegate, ChaosController controller) {
        this.localId = localId;
        this.delegate = delegate;
        this.controller = controller;
    }

    @Override
    public void setReceiver(MessageReceiver receiver) {
        // Wrap the receiver so inbound messages from a partitioned-away peer
        // are dropped too — a partition must be symmetric.
        delegate.setReceiver(msg -> {
            if (controller.shouldDrop(msg.getFrom(), localId)) {
                return;
            }
            receiver.receive(msg);
        });
    }

    @Override
    public void addPeer(long peerId, String address) {
        delegate.addPeer(peerId, address);
    }

    @Override
    public void removePeer(long peerId) {
        delegate.removePeer(peerId);
    }

    @Override
    public void send(long peerId, Eraftpb.Message msg) {
        if (controller.shouldDrop(localId, peerId)) {
            return;
        }
        delegate.send(peerId, msg);
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
