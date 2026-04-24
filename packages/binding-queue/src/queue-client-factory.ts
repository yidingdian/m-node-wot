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

import { ProtocolClient, ProtocolClientFactory, createLoggers } from "@yidingdian/core";
import { QueueProtocolClient, QueueProtocolClientConfig } from "./queue-protocol-client";

const { debug, info, warn } = createLoggers("binding-queue", "queue-client-factory");

/**
 * Factory for creating QueueProtocolClient instances.
 *
 * Manages the lifecycle of queue protocol clients and ensures
 * a single client instance is reused for all queue:// interactions.
 *
 * @example
 * ```javascript
 * const factory = new QueueClientFactory({
 *     FairQueueClass: FairQueue,
 *     redisOptions: { host: 'localhost', port: 6379 },
 * });
 * servient.addClientFactory(factory);
 * ```
 */
export class QueueClientFactory implements ProtocolClientFactory {
    public readonly scheme = "queue";

    private readonly config: QueueProtocolClientConfig;
    private client: QueueProtocolClient | undefined;

    constructor(config: QueueProtocolClientConfig) {
        this.config = config;
        debug("QueueClientFactory created");
    }

    /**
     * Get or create a queue protocol client.
     *
     * The client is lazily created on first access and reused for
     * all subsequent calls. The client auto-starts on first use
     * (Servient does not call start() on protocol clients).
     */
    public getClient(): ProtocolClient {
        if (!this.client) {
            this.client = new QueueProtocolClient(this.config);
            info("Created new QueueProtocolClient instance");
        }
        return this.client;
    }

    /**
     * Return the underlying QueueProtocolClient for direct access to
     * sendQ / recvQ FairQueue instances.
     *
     * Useful when application code needs to call sendQ.waitJobDone() directly
     * (e.g. for non-WoT commands like "getTd") without going through the
     * WoT ConsumedThing API.
     *
     * Returns undefined until the first getClient() call creates the instance.
     */
    public getQueueClient(): QueueProtocolClient | undefined {
        return this.client;
    }

    /**
     * Initialize the factory.
     */
    public init(): boolean {
        debug("QueueClientFactory initialized");
        return true;
    }

    /**
     * Destroy the factory and cleanup resources.
     */
    public destroy(): boolean {
        if (this.client) {
            this.client.stop().catch((err) => {
                warn(`Error stopping client during destroy: ${err instanceof Error ? err.message : String(err)}`);
            });
            this.client = undefined;
            info("QueueClientFactory destroyed");
        }
        return true;
    }
}
