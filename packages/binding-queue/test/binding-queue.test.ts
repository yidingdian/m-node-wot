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
import { Servient } from "@yidingdian/core";
import {
    QueueClientFactory,
    QueueProtocolServer,
    QueueProtocolServerConfig,
    QueueProtocolClientConfig,
    FairQueueJob,
    FairQueueInstance,
} from "../src/index";
import { expect } from "chai";
import "mocha";

/**
 * Mock FairQueue implementation for testing without Redis.
 *
 * Simulates the core FairQueue behavior:
 * - addJob/add: stores jobs in memory
 * - waitJobDone: enqueues and calls processor, returns result
 * - queueJob: buffers and enqueues
 * - Processor callback: called for each job
 */
class MockFairQueue implements FairQueueInstance {
    private processor: ((job: FairQueueJob) => Promise<unknown>) | null;
    private jobs: Map<string, FairQueueJob> = new Map();
    private jobCounter = 0;

    constructor(
        config: Record<string, unknown>,
        processor: ((job: FairQueueJob) => Promise<unknown>) | null,
        _logger?: unknown
    ) {
        this.processor = processor;
    }

    async addJob(
        sn: string,
        jobName: string,
        jobData: Record<string, unknown>,
        jobOpts?: Record<string, unknown>
    ): Promise<{ jobId: string }> {
        const jobId = (jobOpts?.jobId as string) ?? `mock-${++this.jobCounter}`;
        const job: FairQueueJob = {
            id: jobId,
            name: jobName,
            sn,
            data: jobData,
            opts: jobOpts ?? {},
            timestamp: Date.now(),
        };
        this.jobs.set(jobId, job);

        // If there's a processor (consumer mode), process immediately
        if (this.processor) {
            // Process asynchronously to simulate real queue behavior
            setImmediate(async () => {
                try {
                    await this.processor!(job);
                } catch (e) {
                    // Ignore errors in background processing
                }
            });
        }

        return { jobId };
    }

    async waitJobDone(
        sn: string,
        jobName: string,
        jobData: Record<string, unknown>,
        jobOpts?: Record<string, unknown>
    ): Promise<unknown> {
        const jobId = (jobOpts?.jobId as string) ?? `mock-${++this.jobCounter}`;
        const job: FairQueueJob = {
            id: jobId,
            name: jobName,
            sn,
            data: jobData,
            opts: jobOpts ?? {},
            timestamp: Date.now(),
            _waitResult: true,
        };

        if (!this.processor) {
            throw new Error("No processor registered (producer-only mode)");
        }

        return this.processor(job);
    }

    async overrideJob(
        sn: string,
        jobName: string,
        jobData: Record<string, unknown>,
        jobOpts?: Record<string, unknown>
    ): Promise<unknown> {
        return this.waitJobDone(sn, jobName, jobData, jobOpts);
    }

    async queueJob(
        sn: string,
        jobName: string,
        jobData: Record<string, unknown>,
        jobOpts?: Record<string, unknown>
    ): Promise<{ queued: boolean; replaced?: boolean; skipped?: boolean }> {
        await this.addJob(sn, jobName, jobData, jobOpts);
        return { queued: true };
    }

    async add(
        name: string,
        data: Record<string, unknown>,
        opts?: Record<string, unknown>
    ): Promise<{ jobId: string }> {
        const sn = (data.sn as string) || (data.thingId as string) || "global";
        return this.addJob(sn, name, data, opts);
    }

    async getQJob(sn: string, parentSn: string | undefined, jobId: string): Promise<FairQueueJob | null> {
        return this.jobs.get(jobId) ?? null;
    }

    async removeJob(sn: string, jobId: string): Promise<void> {
        this.jobs.delete(jobId);
    }

    async clearQueue(sn: string): Promise<void> {
        this.jobs.clear();
    }

    async stop(): Promise<void> {
        this.jobs.clear();
    }

    async health(): Promise<{ ok: boolean; active: string[] }> {
        return { ok: true, active: [] };
    }
}

// Mock TD for testing
const mockTd: WoT.ThingDescription = {
    "@context": "https://www.w3.org/2022/wot/td/v1.1",
    id: "urn:dev:wot:test-device-123",
    title: "test-device-123",
    securityDefinitions: { nosec_sc: { scheme: "nosec" } },
    security: "nosec_sc",
    properties: {
        brightness: {
            type: "integer",
            minimum: 0,
            maximum: 100,
            observable: true,
            forms: [
                {
                    op: ["readproperty", "writeproperty"],
                    href: "queue://test-device-123/properties/brightness",
                    contentType: "application/json",
                },
            ],
        },
    },
    actions: {
        toggle: {
            input: { type: "object" },
            output: { type: "object" },
            forms: [
                {
                    op: ["invokeaction"],
                    href: "queue://test-device-123/actions/toggle",
                    contentType: "application/json",
                },
            ],
        },
    },
    events: {
        statusChanged: {
            data: { type: "string" },
            forms: [
                {
                    op: ["subscribeevent"],
                    href: "queue://test-device-123/events/statusChanged",
                    contentType: "application/json",
                    subprotocol: "queue-events",
                },
            ],
        },
    },
};

describe("QueueBinding with MockFairQueue", () => {
    describe("QueueProtocolServer", () => {
        let servient: Servient;
        let server: QueueProtocolServer;

        beforeEach(async () => {
            servient = new Servient();
            server = new QueueProtocolServer({
                FairQueueClass: MockFairQueue as any,
                sendQueueName: "test-sendQ",
                recvQueueName: "test-recvQ",
            });
            servient.addServer(server);
        });

        afterEach(async () => {
            await servient.shutdown();
        });

        it("should start and stop without errors", async () => {
            const wot = await servient.start();
            expect(wot).to.be.an("object");
            await servient.shutdown();
        });

        it("should expose a thing and add queue:// forms", async () => {
            const wot = await servient.start();

            const thing = await wot.produce({
                title: "TestDevice",
                properties: {
                    brightness: { type: "integer", observable: true },
                },
                actions: {
                    toggle: {
                        input: { type: "object" },
                        output: { type: "object" },
                    },
                },
                events: {
                    statusChanged: { data: { type: "string" } },
                },
            });

            thing.setPropertyReadHandler("brightness", async () => 42);
            thing.setActionHandler("toggle", async (params) => {
                const input = await params.value();
                return { success: true, input };
            });

            await thing.expose();

            // Check that queue:// forms were added
            const td = thing.getThingDescription();
            const propForms = td.properties?.brightness?.forms ?? [];
            const hasQueuePropForm = propForms.some(
                (f: any) => typeof f.href === "string" && f.href.startsWith("queue://")
            );
            expect(hasQueuePropForm).to.be.true;

            const actionForms = td.actions?.toggle?.forms ?? [];
            const hasQueueActionForm = actionForms.some(
                (f: any) => typeof f.href === "string" && f.href.startsWith("queue://")
            );
            expect(hasQueueActionForm).to.be.true;

            const eventForms = td.events?.statusChanged?.forms ?? [];
            const hasQueueEventForm = eventForms.some(
                (f: any) => typeof f.href === "string" && f.href.startsWith("queue://")
            );
            expect(hasQueueEventForm).to.be.true;
        });

        it("should have accessible sendQ and recvQ", async () => {
            await servient.start();
            expect(server.getSendQueue()).to.not.be.undefined;
            expect(server.getRecvQueue()).to.not.be.undefined;
        });

        it("should destroy a thing", async () => {
            const wot = await servient.start();
            const thing = await wot.produce({ title: "ToDestroy" });
            await thing.expose();

            const result = await server.destroy(thing.getThingDescription().id!);
            expect(result).to.be.true;

            const result2 = await server.destroy("nonexistent-id");
            expect(result2).to.be.false;
        });
    });

    describe("QueueClientFactory", () => {
        it("should create and return a client", () => {
            const factory = new QueueClientFactory({
                FairQueueClass: MockFairQueue as any,
                sendQueueName: "test-sendQ",
                recvQueueName: "test-recvQ",
            });

            const client1 = factory.getClient();
            const client2 = factory.getClient();

            // Should return the same instance
            expect(client1).to.equal(client2);
            expect(client1).to.be.an("object");

            factory.destroy();
        });

        it("should initialize and destroy correctly", () => {
            const factory = new QueueClientFactory({
                FairQueueClass: MockFairQueue as any,
            });

            expect(factory.init()).to.be.true;
            expect(factory.scheme).to.equal("queue");
            expect(factory.destroy()).to.be.true;
        });
    });

    describe("End-to-end Server + Client", () => {
        let serverServient: Servient;
        let clientServient: Servient;
        let server: QueueProtocolServer;

        // Shared mock queues to simulate server and client sharing the same Redis
        let sharedSendProcessor: ((job: FairQueueJob) => Promise<unknown>) | null = null;
        let sharedRecvProcessor: ((job: FairQueueJob) => Promise<unknown>) | null = null;

        /**
         * SharedMockFairQueue: when server creates sendQ (consumer), it registers
         * a processor. When client calls sendQ.waitJobDone (producer), it calls
         * the server's processor directly. This simulates shared Redis.
         */
        class SharedSendQueueMock extends MockFairQueue {
            constructor(config: Record<string, unknown>, processor: any, logger?: unknown) {
                super(config, processor, logger);
                if (processor && !config.producerOnly) {
                    // Server side: register as the consumer
                    sharedSendProcessor = processor;
                }
            }

            async waitJobDone(sn: string, jobName: string, jobData: Record<string, unknown>, jobOpts?: Record<string, unknown>): Promise<unknown> {
                if (sharedSendProcessor) {
                    const job: FairQueueJob = {
                        id: `shared-${Date.now()}`,
                        name: jobName,
                        sn,
                        data: jobData,
                        opts: jobOpts ?? {},
                        timestamp: Date.now(),
                        _waitResult: true,
                    };
                    return sharedSendProcessor(job);
                }
                throw new Error("No processor registered");
            }
        }

        class SharedRecvQueueMock extends MockFairQueue {
            constructor(config: Record<string, unknown>, processor: any, logger?: unknown) {
                super(config, processor, logger);
                if (processor && !config.producerOnly) {
                    sharedRecvProcessor = processor;
                }
            }

            async add(name: string, data: Record<string, unknown>, opts?: Record<string, unknown>): Promise<{ jobId: string }> {
                const sn = (data.sn as string) || (data.thingId as string) || "global";
                const jobId = `recv-${Date.now()}`;
                if (sharedRecvProcessor) {
                    const job: FairQueueJob = {
                        id: jobId,
                        name,
                        sn,
                        data,
                        opts: opts ?? {},
                        timestamp: Date.now(),
                    };
                    // Process asynchronously
                    setImmediate(async () => {
                        try {
                            await sharedRecvProcessor!(job);
                        } catch (e) {
                            // ignore
                        }
                    });
                }
                return { jobId };
            }
        }

        /**
         * Create a FairQueue class that uses the correct shared mock
         * based on the queueName config.
         */
        function createSharedFairQueueClass() {
            return class DynamicMock {
                private delegate: MockFairQueue;

                constructor(config: Record<string, unknown>, processor: any, logger?: unknown) {
                    const queueName = config.queueName as string;
                    if (queueName?.includes("send")) {
                        this.delegate = new SharedSendQueueMock(config, processor, logger);
                    } else {
                        this.delegate = new SharedRecvQueueMock(config, processor, logger);
                    }
                }

                addJob(...args: any[]) { return (this.delegate as any).addJob(...args); }
                waitJobDone(...args: any[]) { return (this.delegate as any).waitJobDone(...args); }
                overrideJob(...args: any[]) { return (this.delegate as any).overrideJob(...args); }
                queueJob(...args: any[]) { return (this.delegate as any).queueJob(...args); }
                add(...args: any[]) { return (this.delegate as any).add(...args); }
                getQJob(...args: any[]) { return (this.delegate as any).getQJob(...args); }
                removeJob(...args: any[]) { return (this.delegate as any).removeJob(...args); }
                clearQueue(...args: any[]) { return (this.delegate as any).clearQueue(...args); }
                stop() { return (this.delegate as any).stop(); }
                health() { return (this.delegate as any).health(); }
            };
        }

        beforeEach(async () => {
            sharedSendProcessor = null;
            sharedRecvProcessor = null;

            const SharedFairQueue = createSharedFairQueueClass();

            // Server side (raft-cloud)
            serverServient = new Servient();
            server = new QueueProtocolServer({
                FairQueueClass: SharedFairQueue as any,
                sendQueueName: "sendQ",
                recvQueueName: "recvQ",
            });
            serverServient.addServer(server);

            // Client side (lightingMain)
            clientServient = new Servient();
            const factory = new QueueClientFactory({
                FairQueueClass: SharedFairQueue as any,
                sendQueueName: "sendQ",
                recvQueueName: "recvQ",
                snResolver: async (thingId: string) => thingId,
            });
            clientServient.addClientFactory(factory);
        });

        afterEach(async () => {
            await serverServient.shutdown();
            await clientServient.shutdown();
        });

        it("should invoke action through queue binding end-to-end", async function () {
            this.timeout(5000);

            // Start server
            const serverWot = await serverServient.start();
            const exposedThing = await serverWot.produce({
                title: "test-device-123",
                actions: {
                    toggle: {
                        input: { type: "object" },
                        output: { type: "object" },
                    },
                },
            });

            exposedThing.setActionHandler("toggle", async (params) => {
                const input = await params.value();
                return { success: true, toggled: true, input };
            });

            await exposedThing.expose();

            // Start client and consume
            const clientWot = await clientServient.start();
            const consumedThing = await clientWot.consume(mockTd);

            expect(consumedThing).to.be.an("object");

            const result = await consumedThing.invokeAction("toggle", { state: true });
            const resultValue = await result.value();

            expect(resultValue).to.deep.include({ success: true, toggled: true });
        });

        it("should read property through queue binding end-to-end", async function () {
            this.timeout(5000);

            const serverWot = await serverServient.start();
            const exposedThing = await serverWot.produce({
                title: "test-device-123",
                properties: {
                    brightness: { type: "integer", observable: true },
                },
            });

            exposedThing.setPropertyReadHandler("brightness", async () => 75);
            await exposedThing.expose();

            const clientWot = await clientServient.start();
            const consumedThing = await clientWot.consume(mockTd);

            const result = await consumedThing.readProperty("brightness");
            const value = await result.value();

            expect(value).to.equal(75);
        });
    });
});
