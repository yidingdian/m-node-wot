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
 * Queue Protocol Server - Exposes Things via FairQueue
 *
 * This server:
 *   1. Creates a FairQueue (sendQ) in consumer mode that processes downlink commands
 *      (readProperty, writeProperty, invokeAction) sent by the client.
 *   2. Creates a FairQueue (recvQ) in producer-only mode to push uplink events
 *      and property changes to the client.
 *   3. Adds queue:// forms to ExposedThing interactions so the TD advertises
 *      the queue binding alongside other bindings.
 *
 * The server does NOT directly handle WoT interaction logic — it delegates to
 * ExposedThing.handleInvokeAction/handleReadProperty/handleWriteProperty, which
 * in turn call the handlers set by the application.
 */

import WoT, { ProtocolServer, ExposedThing, Form, Content, Servient, createLoggers } from "@yidingdian/core";
import { Readable } from "stream";
import {
    QueueConfig,
    QueueForm,
    QueueCommandJobData,
    QueueEventJobData,
    QueueTdJobData,
    FairQueueJob,
    FairQueueInstance,
    parseQueueUri,
    buildQueueUri,
} from "./queue";

const { debug, info, warn, error } = createLoggers("binding-queue", "queue-server");

/**
 * Configuration options for QueueProtocolServer
 */
export interface QueueProtocolServerConfig extends QueueConfig {
    /**
     * FairQueue class constructor.
     * Must be provided — the binding does not bundle FairQueue itself.
     *
     * @example
     * new QueueProtocolServer({ FairQueueClass: FairQueue, ... });
     */
    FairQueueClass: new (
        config: Record<string, unknown>,
        processor: ((job: FairQueueJob) => Promise<unknown>) | null,
        logger?: unknown
    ) => FairQueueInstance;

    /**
     * Handler for application-specific command types not covered by standard
     * WoT operations (invokeAction / readProperty / writeProperty).
     *
     * Called when a job arrives whose `type` field is not a known WoT operation.
     * The handler should return a serialisable result (stored as the job result
     * and returned to the caller via waitJobDone) or throw on failure.
     *
     * @example
     * customCommandHandler: async (job) => {
     *     if (job.data.type === 'getTd') {
     *         return await shadow.getShadowTdBySn(job.data.sn);
     *     }
     * }
     */
    customCommandHandler?: (job: FairQueueJob) => Promise<unknown>;
}

/**
 * Queue Protocol Server implementation using FairQueue.
 *
 * @example
 * ```javascript
 * const { QueueProtocolServer } = require('@yidingdian/binding-queue');
 *
 * const queueServer = new QueueProtocolServer({
 *     FairQueueClass: FairQueue,
 *     redisOptions: cfg.redis.options(),
 *     sendQueueName: 'sendQ',
 *     recvQueueName: 'recvQ',
 *     gatewayRateLimit: {
 *         enabled: true,
 *         defaultLimit: 4,
 *         resolver: (jobData) => jobData.parentSn || null,
 *     },
 * });
 *
 * servient.addServer(queueServer);
 * ```
 */
export class QueueProtocolServer implements ProtocolServer {
    public readonly scheme = "queue";

    private readonly config: QueueProtocolServerConfig;
    private readonly things: Map<string, ExposedThing> = new Map();

    /** Downlink FairQueue — consumer mode, processes commands from client */
    private sendQ?: FairQueueInstance;
    /** Uplink FairQueue — producer-only mode, pushes events to client */
    private recvQ?: FairQueueInstance;

    private servient?: Servient;
    private isRunning = false;

    constructor(config: QueueProtocolServerConfig) {
        this.config = config;
        info(`QueueProtocolServer created (sendQ: ${config.sendQueueName ?? "sendQ"}, recvQ: ${config.recvQueueName ?? "recvQ"})`);
    }

    public getPort(): number {
        return 0; // Not applicable for queue-based binding
    }

    /**
     * Start the server: create FairQueue instances.
     *
     * - sendQ runs in consumer mode with a job processor that dispatches
     *   to the appropriate ExposedThing handler.
     * - recvQ runs in producer-only mode (no scheduler) for pushing events.
     */
    public async start(servient: Servient): Promise<void> {
        if (this.isRunning) {
            warn("QueueProtocolServer already running");
            return;
        }

        this.servient = servient;
        const FairQueueClass = this.config.FairQueueClass;
        const redisOptions = Object.assign(
            { maxRetriesPerRequest: null },
            this.config.redisOptions ?? {}
        );

        // ── Create sendQ (downlink command queue, consumer mode) ────────────
        const sendQConfig: Record<string, unknown> = {
            queueName: this.config.sendQueueName ?? "sendQ",
            maxConcurrency: this.config.maxConcurrency ?? 5000,
            redisOptions,
            waitTTLMs: this.config.waitTTLMs ?? 6000,
            lockTTLMs: this.config.lockTTLMs ?? 30000,
            producerOnly: false,
        };

        if (this.config.gatewayRateLimit) {
            sendQConfig.gatewayRateLimit = this.config.gatewayRateLimit;
        }

        this.sendQ = new FairQueueClass(
            sendQConfig,
            (job: FairQueueJob) => this.processCommand(job),
            this.config.logger
        );

        // ── Create recvQ (uplink event queue, producer-only mode) ───────────
        this.recvQ = new FairQueueClass(
            {
                queueName: this.config.recvQueueName ?? "recvQ",
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

        this.isRunning = true;
        info("QueueProtocolServer started");
    }

    /**
     * Expose a Thing: add queue:// forms to its interactions.
     *
     * This does NOT create per-thing queues or workers. All things share the
     * same sendQ, with FairQueue's per-device isolation handling the routing
     * via the `sn` field in job data.
     */
    public async expose(thing: ExposedThing, tdTemplate?: WoT.ThingDescription): Promise<void> {
        if (!this.isRunning) {
            warn(`QueueProtocolServer not started, cannot expose thing: ${thing.title}`);
            return;
        }

        const thingId = thing.id;
        this.things.set(thingId, thing);

        const baseQueueName = thing.title;
        info(`Exposing thing '${thing.title}' (${thingId}) via queue binding`);

        // ── Actions ─────────────────────────────────────────────────────────
        for (const actionName in thing.actions) {
            if (Object.prototype.hasOwnProperty.call(thing.actions, actionName)) {
                const action = thing.actions[actionName];
                const hasQueueForm = action.forms?.some((f: Form) => f.href.startsWith("queue://"));

                if (!hasQueueForm) {
                    const href = buildQueueUri(baseQueueName, thingId, "actions", actionName);
                    const form: Form = {
                        href,
                        contentType: "application/json",
                        op: ["invokeaction"],
                    };
                    if (!action.forms) {
                        action.forms = [form];
                    } else {
                        action.forms.push(form);
                    }
                    debug(`Action '${actionName}' exposed at ${href}`);
                }
            }
        }

        // ── Properties ──────────────────────────────────────────────────────
        for (const propName in thing.properties) {
            if (Object.prototype.hasOwnProperty.call(thing.properties, propName)) {
                const prop = thing.properties[propName];
                const hasQueueForm = prop.forms?.some((f: Form) => f.href.startsWith("queue://"));

                if (!hasQueueForm) {
                    const href = buildQueueUri(baseQueueName, thingId, "properties", propName);
                    const op: string[] = [];
                    if (!prop.writeOnly) op.push("readproperty");
                    if (!prop.readOnly) op.push("writeproperty");
                    if (prop.observable) op.push("observeproperty", "unobserveproperty");

                    const form: Form = { href, contentType: "application/json", op };
                    if (!prop.forms) {
                        prop.forms = [form];
                    } else {
                        prop.forms.push(form);
                    }
                    debug(`Property '${propName}' exposed at ${href}`);
                }
            }
        }

        // ── Events ──────────────────────────────────────────────────────────
        for (const eventName in thing.events) {
            if (Object.prototype.hasOwnProperty.call(thing.events, eventName)) {
                const event = thing.events[eventName];
                const hasQueueForm = event.forms?.some((f: Form) => f.href.startsWith("queue://"));

                if (!hasQueueForm) {
                    const href = buildQueueUri(baseQueueName, thingId, "events", eventName);
                    const form: Form = {
                        href,
                        contentType: "application/json",
                        op: ["subscribeevent", "unsubscribeevent"],
                        subprotocol: "queue-events",
                    };
                    if (!event.forms) {
                        event.forms = [form];
                    } else {
                        event.forms.push(form);
                    }
                    debug(`Event '${eventName}' exposed at ${href}`);
                }
            }
        }

        // ── Monkey-patch event/property emitters to push to recvQ ───────────
        this.setupEventEmitters(thing);

        info(`Thing '${thing.title}' successfully exposed via queue binding`);
    }

    /**
     * Process a command job from the sendQ FairQueue.
     *
     * This is the FairQueue processor callback. It dispatches to the
     * ExposedThing's handlers based on job.name (readProperty/writeProperty/action).
     *
     * Non-WoT command types (e.g. "getTd") are routed to customCommandHandler
     * before the standard ExposedThing lookup, so they never need a matching Thing.
     */
    private async processCommand(job: FairQueueJob): Promise<unknown> {
        const startTime = Date.now();
        const { name: jobName, data } = job;
        const commandData = data as unknown as QueueCommandJobData;
        const { type, thingId, name } = commandData;

        debug(`Processing ${type ?? jobName} for '${name}' on thing '${thingId}'`);

        // Intercept application-specific command types before thing lookup
        const stdWotTypes = new Set(["invokeAction", "action", "writeProperty", "readProperty"]);
        if (type && !stdWotTypes.has(type)) {
            if (this.config.customCommandHandler) {
                debug(`Delegating custom command '${type}' to customCommandHandler`);
                return await this.config.customCommandHandler(job);
            }
            const errMsg = `Unknown command type '${type}' and no customCommandHandler configured`;
            warn(errMsg);
            throw new Error(errMsg);
        }

        // Find the ExposedThing by thingId
        const thing = this.things.get(thingId);
        if (!thing) {
            // Try finding by title (backward compatibility with existing queue patterns)
            let foundThing: ExposedThing | undefined;
            for (const [, t] of this.things) {
                if (t.title === thingId || data.thingId === t.id) {
                    foundThing = t;
                    break;
                }
            }
            if (!foundThing) {
                const errMsg = `Thing not found: ${thingId}`;
                error(errMsg);
                throw new Error(errMsg);
            }
            return this.dispatchToThing(foundThing, jobName, type, name, data, startTime);
        }

        return this.dispatchToThing(thing, jobName, type, name, data, startTime);
    }

    /**
     * Dispatch a command to the appropriate ExposedThing handler.
     */
    private async dispatchToThing(
        thing: ExposedThing,
        jobName: string,
        type: string | undefined,
        name: string,
        data: Record<string, unknown>,
        startTime: number
    ): Promise<unknown> {
        const effectiveType = type ?? jobName;
        let result: unknown;

        if (effectiveType === "invokeAction" || effectiveType === "action") {
            result = await this.handleInvokeAction(thing, name, data.data ?? data.input);
        } else if (effectiveType === "writeProperty") {
            result = await this.handleWriteProperty(thing, name, data.data);
        } else if (effectiveType === "readProperty") {
            result = await this.handleReadProperty(thing, name);
        } else {
            warn(`Unknown operation type: ${effectiveType}`);
            throw new Error(`Unknown operation type: ${effectiveType}`);
        }

        const elapsed = Date.now() - startTime;
        debug(`${effectiveType} for '${name}' completed in ${elapsed}ms`);

        return result;
    }

    /**
     * Handle invokeAction: delegate to ExposedThing.handleInvokeAction
     */
    private async handleInvokeAction(thing: ExposedThing, name: string, input: unknown): Promise<unknown> {
        const forms = thing.actions[name]?.forms ?? [];
        const formIndex = forms.findIndex((f: Form) => f.href.startsWith("queue://"));
        const content = new Content("application/json", Readable.from([JSON.stringify(input ?? null)]));
        const output = await thing.handleInvokeAction(name, content, { formIndex: formIndex >= 0 ? formIndex : 0 });
        if (output) {
            return await this.readContent(output);
        }
        return null;
    }

    /**
     * Handle writeProperty: delegate to ExposedThing.handleWriteProperty
     */
    private async handleWriteProperty(thing: ExposedThing, name: string, input: unknown): Promise<{ success: boolean }> {
        const forms = thing.properties[name]?.forms ?? [];
        const formIndex = forms.findIndex((f: Form) => f.href.startsWith("queue://"));
        const content = new Content("application/json", Readable.from([JSON.stringify(input)]));
        await thing.handleWriteProperty(name, content, { formIndex: formIndex >= 0 ? formIndex : 0 });
        return { success: true };
    }

    /**
     * Handle readProperty: delegate to ExposedThing.handleReadProperty
     */
    private async handleReadProperty(thing: ExposedThing, name: string): Promise<unknown> {
        const forms = thing.properties[name]?.forms ?? [];
        const formIndex = forms.findIndex((f: Form) => f.href.startsWith("queue://"));
        const output = await thing.handleReadProperty(name, { formIndex: formIndex >= 0 ? formIndex : 0 });
        return await this.readContent(output);
    }

    /**
     * Monkey-patch emitEvent and emitPropertyChange to also push to recvQ.
     *
     * When the shadow device receives an MQTT event from the real device,
     * it calls emitEvent(). This patch ensures the event also gets enqueued
     * in recvQ for lightingMain to consume.
     */
    private setupEventEmitters(thing: ExposedThing): void {
        const originalEmitEvent = thing.emitEvent.bind(thing);
        thing.emitEvent = (name: string, data: WoT.InteractionInput) => {
            // Still call original emitter (for other bindings like HTTP/SSE)
            originalEmitEvent(name, data);

            // Push to recvQ
            if (this.recvQ) {
                const sn = (thing as unknown as Record<string, unknown>).title as string ?? thing.id;
                const payload: QueueEventJobData = {
                    title: thing.title,
                    thingId: thing.id,
                    name,
                    data: data as unknown,
                    timestamp: Date.now(),
                };

                this.recvQ.add("event", payload as unknown as Record<string, unknown>, {
                    removeOnComplete: true,
                }).catch((err: Error) => {
                    error(`Failed to emit event '${name}' to recvQ: ${err.message}`);
                });
            }
        };

        const originalEmitPropertyChange = thing.emitPropertyChange.bind(thing);
        thing.emitPropertyChange = async (name: string, data?: WoT.InteractionInput) => {
            await originalEmitPropertyChange(name, data);

            if (this.recvQ && data !== undefined) {
                const payload: QueueEventJobData = {
                    title: thing.title,
                    thingId: thing.id,
                    name,
                    data: data as unknown,
                    timestamp: Date.now(),
                };

                this.recvQ.add("property", payload as unknown as Record<string, unknown>, {
                    removeOnComplete: true,
                }).catch((err: Error) => {
                    error(`Failed to emit property change '${name}' to recvQ: ${err.message}`);
                });
            }
        };
    }

    /**
     * Read Content and parse as JSON
     */
    private async readContent(content: Content): Promise<unknown> {
        const buffer = await content.toBuffer();
        if (buffer.length === 0) return null;
        try {
            return JSON.parse(buffer.toString());
        } catch {
            return buffer.toString();
        }
    }

    /**
     * Push a TD change notification to recvQ.
     *
     * Called by the application (e.g. raft-cloud) when a shadow TD is
     * created, updated, or deleted.  lightingMain's recvQ consumer receives
     * the job and updates its local TD cache, replacing the SSE/HTTP path.
     *
     * @param thingId   Full Thing URN
     * @param sn        Device serial number (FairQueue routing key)
     * @param title     Shadow TD title (used as productId by the consumer)
     * @param td        Full Thing Description (pass empty object for deletions)
     * @param operation "thing_created" | "thing_updated" | "thing_deleted"
     * @param shadowDevice  Optional device metadata for startup status sync
     */
    public async notifyTdUpdate(
        thingId: string,
        sn: string,
        title: string,
        td: Record<string, unknown>,
        operation: string,
        shadowDevice?: Record<string, unknown>
    ): Promise<void> {
        if (!this.isRunning || !this.recvQ) {
            warn(`notifyTdUpdate called but recvQ is not ready (isRunning=${this.isRunning})`);
            return;
        }

        const payload: QueueTdJobData = {
            thingId,
            sn,
            title,
            td,
            operation: operation as QueueTdJobData["operation"],
            shadowDevice: shadowDevice as QueueTdJobData["shadowDevice"],
            timestamp: Date.now(),
        };

        await this.recvQ.add("td", payload as unknown as Record<string, unknown>, {
            removeOnComplete: true,
        });

        debug(`notifyTdUpdate: pushed '${operation}' for ${thingId} (sn=${sn})`);
    }

    /**
     * Destroy a specific thing (remove from internal map).
     */
    public async destroy(thingId: string): Promise<boolean> {
        const thing = this.things.get(thingId);
        if (!thing) {
            debug(`Thing ${thingId} not found for destruction`);
            return false;
        }

        info(`Destroying thing: ${thing.title} (${thingId})`);
        this.things.delete(thingId);
        return true;
    }

    /**
     * Stop the server: shut down both FairQueue instances.
     */
    public async stop(): Promise<void> {
        if (!this.isRunning) {
            return;
        }

        info("Stopping QueueProtocolServer...");

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

        this.things.clear();
        this.isRunning = false;
        info("QueueProtocolServer stopped");
    }

    // ── Public accessors for direct FairQueue usage ─────────────────────────

    /**
     * Get the send (downlink) FairQueue instance.
     * Useful for direct access to addJob/waitJobDone/queueJob etc.
     */
    public getSendQueue(): FairQueueInstance | undefined {
        return this.sendQ;
    }

    /**
     * Get the receive (uplink) FairQueue instance.
     * Useful for direct access to add events.
     */
    public getRecvQueue(): FairQueueInstance | undefined {
        return this.recvQ;
    }
}
