/********************************************************************************
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the W3C Software Notice and
 * Document License (2015-05-13) which is available at
 * https://www.w3.org/Consortium/Legal/2015/copyright-software-and-document.
 *
 * SPDX-License-Identifier: EPL-2.0 OR W3C-20150513
 ********************************************************************************/

/**
 * @packageDocumentation
 *
 * Queue Binding for node-wot (Web of Things)
 *
 * This package provides a queue-based protocol binding for WoT,
 * using FairQueue for asynchronous, fair-scheduled communication
 * between Thing consumers and producers in IoT systems.
 *
 * Key features:
 *   - Per-device fair scheduling via Redis ZSET + round-robin active ring
 *   - Per-device serial execution with cross-device parallelism
 *   - Gateway-level rate limiting (sliding time window)
 *   - Dedup/override for write operations (queueJob)
 *   - Synchronous request-response (waitJobDone) for read/action operations
 *   - Memory-buffered writes with configurable flush interval
 *   - Stalled device recovery via watchdog
 *
 * Architecture:
 *   - Server: QueueProtocolServer exposes Things via sendQ (consumer) + recvQ (producer)
 *   - Client: QueueClientFactory creates QueueProtocolClient with sendQ (producer) + recvQ (consumer)
 *
 * @example
 * ```typescript
 * import { Servient } from "@yidingdian/core";
 * import { QueueClientFactory, QueueProtocolServer } from "@yidingdian/binding-queue";
 *
 * // Server side - exposing Things
 * const server = new QueueProtocolServer({
 *     FairQueueClass: FairQueue,
 *     redisOptions: { host: "localhost", port: 6379 },
 *     sendQueueName: "sendQ",
 *     recvQueueName: "recvQ",
 *     gatewayRateLimit: { enabled: true, defaultLimit: 4 },
 * });
 * servient.addServer(server);
 *
 * // Client side - consuming Things
 * const factory = new QueueClientFactory({
 *     FairQueueClass: FairQueue,
 *     redisOptions: { host: "localhost", port: 6379 },
 *     sendQueueName: "sendQ",
 *     recvQueueName: "recvQ",
 *     snResolver: async (thingId) => lookupSn(thingId),
 * });
 * servient.addClientFactory(factory);
 * ```
 */

export * from "./queue";
export * from "./queue-client-factory";
export * from "./queue-protocol-client";
export * from "./queue-protocol-server";
