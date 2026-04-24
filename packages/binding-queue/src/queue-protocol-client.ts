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
 * Queue Protocol Client - Consumes Things via FairQueue
 *
 * This client:
 *   1. Creates a FairQueue (sendQ) in producer-only mode to send downlink commands
 *      (readProperty, writeProperty, invokeAction) to the server.
 *   2. Creates a FairQueue (recvQ) in consumer mode to receive uplink events
 *      and property changes from the server.
 *
 * Key method mapping:
 *   - readResource()    → sendQ.waitJobDone(sn, "readProperty", ...)
 *   - writeResource()   → sendQ.queueJob(sn, "writeProperty", ...)
 *   - invokeResource()  → sendQ.waitJobDone(sn, "action", ...) or sendQ.overrideJob(...)
 *   - subscribeResource() → registers a local callback, dispatched by recvQ processor
 */

import { ProtocolClient, Content, Form, SecurityScheme, createLoggers } from "@yidingdian/core";
import { Subscription } from "rxjs/Subscription";
import { Readable } from "stream";
import {
    QueueConfig,
    QueueForm,
    QueueCommandJobData,
    QueueEventJobData,
    FairQueueJob,
    FairQueueInstance,
    parseQueueUri,
} from "./queue";

const { debug, info, warn, error } = createLoggers("binding-queue", "queue-client");

/**
 * Subscriber callback interface for event/property subscriptions.
 */
interface Subscriber {
    next: (content: Content) => void;
    error?: (err: Error) => void;
    complete?: () => void;
}

/**
 * Extended config for the client, requiring FairQueue class injection.
 */
export interface QueueProtocolClientConfig extends QueueConfig {
    /**
     * FairQueue class constructor.
     * Must be provided — the binding does not bundle FairQueue itself.
     */
    FairQueueClass: new (
        config: Record<string, unknown>,
        processor: ((job: FairQueueJob) => Promise<unknown>) | null,
        logger?: unknown
    ) => FairQueueInstance;

    /**
     * Resolver to extract the device SN from a queue:// URI or form.
     *
     * Since the client doesn't necessarily know the device SN from the form URL alone,
     * this resolver allows the application to provide custom SN extraction logic.
     *
     * If not provided, defaults to using the thingId (hostname) from the queue:// URI.
     *
     * @example
     * snResolver: (thingId, name) => {
     *     // Look up SN from your device registry
     *     return deviceRegistry.getSnByThingId(thingId);
     * }
     */
    snResolver?: (thingId: string, interactionName: string) => string | Promise<string>;

    /**
     * Resolver to extract parentSn (gateway SN) for rate limiting.
     */
    parentSnResolver?: (thingId: string) => string | undefined | Promise<string | undefined>;

    /**
     * Skip creating the recvQ (uplink event queue) consumer.
     *
     * When true, the client only creates sendQ in producer-only mode.
     * Useful when the application has its own independent recvQ consumer.
     *
     * Default: false
     */
    skipRecvQueue?: boolean;
}

/**
 * Queue Protocol Client implementation using FairQueue.
 *
 * @example
 * ```javascript
 * const { QueueClientFactory } = require('@yidingdian/binding-queue');
 *
 * const factory = new QueueClientFactory({
 *     FairQueueClass: FairQueue,
 *     redisOptions: { host: 'localhost', port: 6379 },
 *     sendQueueName: 'sendQ',
 *     recvQueueName: 'recvQ',
 *     snResolver: async (thingId, name) => {
 *         const device = await db.findByThingId(thingId);
 *         return device.sn;
 *     },
 *     parentSnResolver: async (thingId) => {
 *         const device = await db.findByThingId(thingId);
 *         return device.parentSn;
 *     },
 * });
 * servient.addClientFactory(factory);
 * ```
 */
export class QueueProtocolClient implements ProtocolClient {
    private readonly config: QueueProtocolClientConfig;

    /** Downlink FairQueue — producer-only mode, sends commands to server */
    private sendQ?: FairQueueInstance;
    /** Uplink FairQueue — consumer mode, receives events from server */
    private recvQ?: FairQueueInstance;

    /** Event/property subscribers keyed by "thingId/interactionName" */
    private readonly subscriptions = new Map<string, Subscriber[]>();

    private isRunning = false;

    constructor(config: QueueProtocolClientConfig) {
        this.config = config;
        info(`QueueProtocolClient created (sendQ: ${config.sendQueueName ?? "sendQ"}, recvQ: ${config.recvQueueName ?? "recvQ"})`);
    }

    /**
     * Start the client: create FairQueue instances.
     *
     * - sendQ runs in producer-only mode for sending commands.
     * - recvQ runs in consumer mode with a processor that dispatches
     *   events/properties to local subscribers.
     */
    public async start(): Promise<void> {
        if (this.isRunning) {
            return;
        }

        const FairQueueClass = this.config.FairQueueClass;
        const redisOptions = Object.assign(
            { maxRetriesPerRequest: null },
            this.config.redisOptions ?? {}
        );

        // ── Create sendQ (producer-only, for sending commands) ──────────────
        this.sendQ = new FairQueueClass(
            {
                queueName: this.config.sendQueueName ?? "sendQ",
                maxConcurrency: this.config.maxConcurrency ?? 5000,
                redisOptions,
                waitTTLMs: this.config.waitTTLMs ?? 6000,
                lockTTLMs: this.config.lockTTLMs ?? 30000,
                producerOnly: true,
                gatewayRateLimit: { enabled: false },
            },
            null,
            this.config.logger
        );

        // ── Create recvQ (consumer mode, for receiving events) ──────────────
        if (!this.config.skipRecvQueue) {
            this.recvQ = new FairQueueClass(
                {
                    queueName: this.config.recvQueueName ?? "recvQ",
                    maxConcurrency: this.config.maxConcurrency ?? 5000,
                    redisOptions,
                    waitTTLMs: this.config.waitTTLMs ?? 6000,
                    lockTTLMs: this.config.lockTTLMs ?? 30000,
                    producerOnly: false,
                    gatewayRateLimit: { enabled: false },
                },
                (job: FairQueueJob) => this.processEvent(job),
                this.config.logger
            );
        }

        this.isRunning = true;
        info("QueueProtocolClient started");
    }

    /**
     * Process an event/property job from recvQ.
     *
     * This is the FairQueue processor callback for the uplink queue.
     * It dispatches to registered subscribers.
     */
    private async processEvent(job: FairQueueJob): Promise<unknown> {
        const eventData = job.data as unknown as QueueEventJobData;
        const { thingId, name, data } = eventData;
        const topic = `${thingId}/${name}`;

        debug(`Received ${job.name} for topic: ${topic}`);

        const subscribers = this.subscriptions.get(topic);
        if (!subscribers || subscribers.length === 0) {
            debug(`No subscribers for topic: ${topic}`);
            return null;
        }

        for (const sub of subscribers) {
            try {
                const content = this.createContentFromData(data);
                sub.next(content);
            } catch (e) {
                const err = e instanceof Error ? e : new Error(String(e));
                error(`Error dispatching event for ${topic}: ${err.message}`);
                if (sub.error) sub.error(err);
            }
        }

        return null;
    }

    // ─── ProtocolClient Interface ───────────────────────────────────────────

    /**
     * Read a property value via sendQ.waitJobDone.
     */
    public async readResource(form: Form): Promise<Content> {
        this.ensureRunning();

        const { thingId, interactionName } = parseQueueUri(form.href);
        const name = interactionName ?? "";
        const queueForm = form as QueueForm;

        const sn = await this.resolveSn(thingId, name);
        const parentSn = await this.resolveParentSn(thingId);

        const jobData: Record<string, unknown> = {
            type: "readProperty",
            thingId,
            sn,
            parentSn,
            name,
            timestamp: Date.now(),
        };

        const jobOpts: Record<string, unknown> = {
            jobId: `${thingId}/${name}/read`,
            attempts: queueForm["queue:retries"] ?? 5,
            backoff: 1000,
            priority: 2,
        };

        if (queueForm["queue:timeout"]) {
            jobOpts.waitTTL = queueForm["queue:timeout"];
        }

        debug(`readResource: ${thingId}/${name} via sendQ.waitJobDone`);
        const result = await this.sendQ!.waitJobDone(sn, "readProperty", jobData, jobOpts);
        return this.createContentFromData(result);
    }

    /**
     * Write a property value via sendQ.queueJob (buffered, dedup by jobId).
     */
    public async writeResource(form: Form, content: Content): Promise<void | Content> {
        this.ensureRunning();

        const { thingId, interactionName } = parseQueueUri(form.href);
        const name = interactionName ?? "";
        const queueForm = form as QueueForm;

        const sn = await this.resolveSn(thingId, name);
        const parentSn = await this.resolveParentSn(thingId);

        let inputData: unknown = null;
        if (content) {
            const buffer = await content.toBuffer();
            if (buffer.length > 0) {
                try {
                    inputData = JSON.parse(buffer.toString());
                } catch {
                    inputData = buffer.toString();
                }
            }
        }

        const jobData: Record<string, unknown> = {
            type: "writeProperty",
            thingId,
            sn,
            parentSn,
            name,
            data: inputData,
            timestamp: Date.now(),
        };

        const jobOpts: Record<string, unknown> = {
            jobId: `${thingId}/${name}/write`,
            removeOnComplete: true,
            removeOnFail: true,
            attempts: queueForm["queue:retries"] ?? 5,
            backoff: 1000,
        };

        if (queueForm["queue:priority"]) {
            jobOpts.priority = queueForm["queue:priority"];
        }

        if (queueForm["queue:delay"]) {
            jobOpts.delay = queueForm["queue:delay"];
        }

        debug(`writeResource: ${thingId}/${name} via sendQ.queueJob`);
        await this.sendQ!.queueJob(sn, "writeProperty", jobData, jobOpts);
        return;
    }

    /**
     * Invoke an action via sendQ.waitJobDone (synchronous, waits for result).
     */
    public async invokeResource(form: Form, content?: Content): Promise<Content> {
        this.ensureRunning();

        const { thingId, interactionName } = parseQueueUri(form.href);
        const name = interactionName ?? "";
        const queueForm = form as QueueForm;

        const sn = await this.resolveSn(thingId, name);
        const parentSn = await this.resolveParentSn(thingId);

        let inputData: unknown = null;
        if (content) {
            const buffer = await content.toBuffer();
            if (buffer.length > 0) {
                try {
                    inputData = JSON.parse(buffer.toString());
                } catch {
                    inputData = buffer.toString();
                }
            }
        }

        const jobData: Record<string, unknown> = {
            type: "invokeAction",
            thingId,
            sn,
            parentSn,
            name,
            input: inputData,
            data: inputData,
            timestamp: Date.now(),
        };

        const jobOpts: Record<string, unknown> = {};

        if (queueForm["queue:timeout"]) {
            jobOpts.waitTTL = queueForm["queue:timeout"];
        }

        debug(`invokeResource: ${thingId}/${name} via sendQ.waitJobDone`);
        const result = await this.sendQ!.waitJobDone(sn, "action", jobData, jobOpts);
        return this.createContentFromData(result);
    }

    /**
     * Subscribe to events or property observations from recvQ.
     */
    public async subscribeResource(
        form: Form,
        next: (value: Content) => void,
        errorCallback?: (err: Error) => void,
        complete?: () => void
    ): Promise<Subscription> {
        const { thingId, interactionName } = parseQueueUri(form.href);
        const eventName = interactionName ?? "";
        const topic = `${thingId}/${eventName}`;

        debug(`Subscribing to topic: ${topic}`);

        if (!this.subscriptions.has(topic)) {
            this.subscriptions.set(topic, []);
        }

        const subscriber: Subscriber = { next, error: errorCallback, complete };
        this.subscriptions.get(topic)?.push(subscriber);

        const subscription = {
            closed: false,
            unsubscribe: () => {
                const subs = this.subscriptions.get(topic);
                if (subs) {
                    const index = subs.indexOf(subscriber);
                    if (index > -1) {
                        subs.splice(index, 1);
                        debug(`Unsubscribed from topic: ${topic}`);
                    }
                    if (subs.length === 0) {
                        this.subscriptions.delete(topic);
                    }
                }
            },
        } as Subscription;

        return subscription;
    }

    /**
     * Unlink/unsubscribe from a resource.
     */
    public async unlinkResource(form: Form): Promise<void> {
        const { thingId, interactionName } = parseQueueUri(form.href);
        const topic = `${thingId}/${interactionName ?? ""}`;

        if (this.subscriptions.has(topic)) {
            this.subscriptions.delete(topic);
            debug(`Unlinked resource: ${topic}`);
        }
    }

    /**
     * Request a Thing Description — not supported via queue.
     */
    public async requestThingDescription(uri: string): Promise<Content> {
        warn(`requestThingDescription not supported via queue binding: ${uri}`);
        throw new Error("Thing Description discovery not supported via queue binding");
    }

    /**
     * Set security credentials — queue binding uses Redis auth if configured.
     */
    public setSecurity(metadata: Array<SecurityScheme>, credentials?: unknown): boolean {
        debug("setSecurity called - queue binding uses Redis authentication if configured");
        return true;
    }

    /**
     * Stop the client: shut down both FairQueue instances.
     */
    public async stop(): Promise<void> {
        if (!this.isRunning) {
            return;
        }

        info("Stopping QueueProtocolClient...");

        if (this.sendQ) {
            try {
                await this.sendQ.stop();
            } catch (err) {
                warn(`Error stopping sendQ: ${err instanceof Error ? err.message : String(err)}`);
            }
            this.sendQ = undefined;
        }

        if (this.recvQ) {
            try {
                await this.recvQ.stop();
            } catch (err) {
                warn(`Error stopping recvQ: ${err instanceof Error ? err.message : String(err)}`);
            }
            this.recvQ = undefined;
        }

        this.subscriptions.clear();
        this.isRunning = false;
        info("QueueProtocolClient stopped");
    }

    // ─── Helper Methods ─────────────────────────────────────────────────────

    private ensureRunning(): void {
        if (!this.isRunning || !this.sendQ) {
            // Auto-start: Servient does not call start() on protocol clients,
            // only on servers. FairQueue constructor is synchronous, so we can
            // call start() here safely even though it's declared async.
            info("Auto-starting QueueProtocolClient (Servient does not call client.start())");
            this.start().catch((err) => {
                error(`Failed to auto-start: ${err instanceof Error ? err.message : String(err)}`);
            });
            if (!this.isRunning || !this.sendQ) {
                throw new Error("QueueProtocolClient failed to start.");
            }
        }
    }

    /**
     * Resolve device SN from thingId using the configured resolver.
     * Falls back to thingId itself if no resolver is configured.
     */
    private async resolveSn(thingId: string, interactionName: string): Promise<string> {
        if (this.config.snResolver) {
            return this.config.snResolver(thingId, interactionName);
        }
        return thingId;
    }

    /**
     * Resolve parent SN (gateway SN) from thingId using the configured resolver.
     */
    private async resolveParentSn(thingId: string): Promise<string | undefined> {
        if (this.config.parentSnResolver) {
            return this.config.parentSnResolver(thingId);
        }
        return undefined;
    }

    /**
     * Create Content object from arbitrary data.
     */
    private createContentFromData(data: unknown): Content {
        const jsonData = JSON.stringify(data ?? null);
        const stream = Readable.from([Buffer.from(jsonData)]);
        return new Content("application/json", stream);
    }

    // ─── Public Accessors ───────────────────────────────────────────────────

    /**
     * Get the send (downlink) FairQueue instance for direct access.
     */
    public getSendQueue(): FairQueueInstance | undefined {
        return this.sendQ;
    }

    /**
     * Get the receive (uplink) FairQueue instance for direct access.
     */
    public getRecvQueue(): FairQueueInstance | undefined {
        return this.recvQ;
    }
}
