/********************************************************************************
 * Copyright (c) 2018 Contributors to the Eclipse Foundation
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
 * Protocol test suite to test protocol implementations
 */

import {
    ProtocolClient,
    Content,
    DefaultContent,
    createLoggers,
    ContentSerdes,
    Form,
    SecurityScheme,
} from "@node-wot/core";
import * as mqtt from "mqtt";
import { MqttClientConfig, MqttForm } from "./mqtt";
import * as url from "url";
import { Subscription } from "rxjs/Subscription";
import { Readable } from "stream";
import MQTTMessagePool from "./mqtt-message-pool";
import { mapQoS } from "./util";

const { debug, info, warn } = createLoggers("binding-mqtt", "mqtt-client");

const DEFAULT_TIMEOUT = 5000;

declare interface MqttClientSecurityParameters {
    username: string;
    password: string;
}

interface PendingRequest {
    contentType: string;
    resolve: (value: Content) => void;
    reject: (reason: Error) => void;
    timer?: NodeJS.Timeout;
}

function generateCorrelationData(): Buffer {
    return Buffer.from(Math.random().toString(16).substring(2, 10));
}

export default class MqttClient implements ProtocolClient {
    private scheme: string;
    private pools: Map<string, MQTTMessagePool> = new Map();
    // pool -> responseTopic -> correlationDataHex -> PendingRequest
    private pendingByPool: Map<MQTTMessagePool, Map<string, Map<string, PendingRequest>>> = new Map();

    constructor(
        private config: MqttClientConfig = {},
        secure = false
    ) {
        this.scheme = "mqtt" + (secure ? "s" : "");
        // Resolve shard count: config wins, then env, then default 1 (off).
        this.shards = Math.max(1, Number(config.connectionShards ?? process.env.MQTT_CONN_SHARDS) || 1);
    }

    private client?: mqtt.MqttClient;

    private ensureV5Properties(options: mqtt.IClientPublishOptions) {
        if (this.config.protocolVersion === 5) {
            if (!options.properties) {
                options.properties = {};
            }
            if (!options.properties.correlationData) {
                options.properties.correlationData = generateCorrelationData();
            }
            if (!options.properties.userProperties) {
                options.properties.userProperties = {};
            }
            if (!options.properties.userProperties.timestamp) {
                options.properties.userProperties.timestamp = new Date().toISOString();
            }
        }
    }

    // ── Connection sharding ────────────────────────────────────────────────
    // Default 1 = behave exactly as before (one pool/connection per broker URI).
    // With shards>1 the per-device request/response load is spread across N
    // persistent connections, hashed by the device SN segment of the topic so a
    // given device's request + response always land on the same pool. The shard
    // count is resolved in the constructor (config.connectionShards, then the
    // MQTT_CONN_SHARDS env var, then 1). The diagnostic logging below is
    // intentionally lightweight: it only fires while sharding is enabled and is
    // bounded by the shard count.
    private readonly shards: number;
    private shardActiveLogged = false;

    // topic form: raft/<product>/<SN>/... — hash the SN segment into [0, shards).
    private shardKey(brokerUri: string, topic?: string | string[]): string {
        if (this.shards <= 1) return brokerUri;
        const t = Array.isArray(topic) ? topic[0] : topic;
        if (!t) return `${brokerUri}#0`;
        const seg = t.split("/");
        const sn = seg.length > 2 ? seg[2] : t;
        let h = 0;
        for (let i = 0; i < sn.length; i++) h = Math.imul(h, 31) + sn.charCodeAt(i);
        return `${brokerUri}#${Math.abs(h | 0) % this.shards}`;
    }

    private async getPool(brokerUri: string, topic?: string | string[]): Promise<MQTTMessagePool> {
        const key = this.shardKey(brokerUri, topic);
        let pool = this.pools.get(key);
        if (pool == null) {
            if (this.shards > 1 && !this.shardActiveLogged) {
                this.shardActiveLogged = true;
                info(`[mqtt-shard] enabled: shards=${this.shards}, routing by hash(SN)`);
            }
            pool = new MQTTMessagePool();
            this.pools.set(key, pool);
            await pool.connect(brokerUri, this.config);
            if (this.shards > 1) {
                info(`[mqtt-shard] pool connected: ${key} (${this.pools.size}/${this.shards} up)`);
            }
            return pool;
        }
        await pool.connect(brokerUri, this.config);
        return pool;
    }

    private pendingForPool(pool: MQTTMessagePool): Map<string, Map<string, PendingRequest>> {
        let topicMap = this.pendingByPool.get(pool);
        if (!topicMap) {
            topicMap = new Map();
            this.pendingByPool.set(pool, topicMap);
        }
        return topicMap;
    }

    private async ensureResponseListener(pool: MQTTMessagePool, responseTopic: string): Promise<Map<string, PendingRequest>> {
        const topicMap = this.pendingForPool(pool);
        const existing = topicMap.get(responseTopic);
        if (existing) return existing;

        const reqMap = new Map<string, PendingRequest>();
        topicMap.set(responseTopic, reqMap);

        try {
            await pool.subscribe(
                responseTopic,
                (_topic: string, message: Buffer, packet: mqtt.IPublishPacket) => {
                    this.dispatchResponse(pool, responseTopic, message, packet);
                },
                (err: Error) => {
                    this.failPending(pool, responseTopic, err);
                }
            );
        } catch (err) {
            // rollback registry on subscribe failure so a retry can re-subscribe
            topicMap.delete(responseTopic);
            throw err;
        }
        return reqMap;
    }

    private dispatchResponse(
        pool: MQTTMessagePool,
        responseTopic: string,
        message: Buffer,
        packet: mqtt.IPublishPacket
    ): void {
        const topicMap = this.pendingByPool.get(pool);
        const reqMap = topicMap?.get(responseTopic);
        if (!reqMap) return;

        const corr = packet.properties?.correlationData;
        let entry: PendingRequest | undefined;
        let entryKey: string | undefined;

        if (corr) {
            entryKey = (corr as Buffer).toString("hex");
            entry = reqMap.get(entryKey);
            if (!entry) {
                debug(
                    `Dropping response on '${responseTopic}' with unmatched correlationData ${entryKey}; ` +
                        `${reqMap.size} pending`
                );
                return;
            }
        } else {
            // Backward compat: if exactly one request is pending, treat the response as its reply.
            if (reqMap.size === 1) {
                const it = reqMap.entries().next().value as [string, PendingRequest];
                entryKey = it[0];
                entry = it[1];
            } else if (reqMap.size === 0) {
                return;
            } else {
                warn(
                    `Received response on '${responseTopic}' without correlationData but ${reqMap.size} pending; dropping`
                );
                return;
            }
        }

        if (entry.timer) clearTimeout(entry.timer);
        reqMap.delete(entryKey!);
        const contentType = entry.contentType;
        // Resolve before potentially unsubscribing so the consumer is unblocked first.
        entry.resolve(new Content(contentType, Readable.from(message), packet));
        this.maybeReleaseResponseTopic(pool, responseTopic);
    }

    private failPending(pool: MQTTMessagePool, responseTopic: string, err: Error): void {
        const topicMap = this.pendingByPool.get(pool);
        const reqMap = topicMap?.get(responseTopic);
        if (!reqMap) return;
        topicMap!.delete(responseTopic);
        for (const entry of reqMap.values()) {
            if (entry.timer) clearTimeout(entry.timer);
            entry.reject(err);
        }
        reqMap.clear();
    }

    private maybeReleaseResponseTopic(pool: MQTTMessagePool, responseTopic: string): void {
        const topicMap = this.pendingByPool.get(pool);
        const reqMap = topicMap?.get(responseTopic);
        if (!reqMap || reqMap.size > 0) return;
        topicMap!.delete(responseTopic);
        pool.unsubscribe(responseTopic).catch((e: Error) => {
            warn(`Failed to unsubscribe response topic '${responseTopic}': ${e.message}`);
        });
    }

    private async publishAndWait(
        pool: MQTTMessagePool,
        topic: string,
        buffer: Buffer,
        options: mqtt.IClientPublishOptions,
        responseTopic: string,
        contentType: string
    ): Promise<Content> {
        this.ensureV5Properties(options);
        const correlationData = options.properties!.correlationData as Buffer;
        const corrKey = correlationData.toString("hex");

        const reqMap = await this.ensureResponseListener(pool, responseTopic);

        if (reqMap.has(corrKey)) {
            // Astronomically unlikely with 128-bit random, but be defensive.
            throw new Error(`correlationData collision on '${responseTopic}'`);
        }

        const promise = new Promise<Content>((resolve, reject) => {
            const entry: PendingRequest = { contentType, resolve, reject };
            entry.timer = setTimeout(() => {
                if (reqMap.get(corrKey) === entry) {
                    reqMap.delete(corrKey);
                    this.maybeReleaseResponseTopic(pool, responseTopic);
                }
                reject(new Error(`Timeout waiting for response on topic ${responseTopic}`));
            }, this.config.timeout ?? DEFAULT_TIMEOUT);
            reqMap.set(corrKey, entry);
        });
        // Prevent "unhandledRejection" if the timeout fires while we are still
        // awaiting pool.publish (waiting for PUBACK from broker).
        promise.catch(() => {});

        try {
            await pool.publish(topic, buffer, options);
        } catch (err) {
            const entry = reqMap.get(corrKey);
            if (entry) {
                if (entry.timer) clearTimeout(entry.timer);
                reqMap.delete(corrKey);
                this.maybeReleaseResponseTopic(pool, responseTopic);
            }
            throw err;
        }

        return promise;
    }

    public async subscribeResource(
        form: MqttForm,
        next: (value: Content) => void,
        error?: (error: Error) => void,
        complete?: () => void
    ): Promise<Subscription> {
        const contentType = form.contentType ?? ContentSerdes.DEFAULT;
        const requestUri = new url.URL(form.href);
        const brokerUri: string = `${this.scheme}://` + requestUri.host;
        // Keeping the path as the topic for compatibility reasons.
        // Current specification allows only form["mqv:filter"]. If href has no path (empty string),
        // `requestUri.pathname.slice(1)` returns '' which is not nullish — use explicit empty-check.
        const _path = requestUri.pathname.slice(1) || "";
        const filter = _path.length ? _path : form["mqv:filter"];

        if (!filter) {
            throw new Error("No topic or filter provided");
        }
        if (Array.isArray(filter)) {
            // pool.subscribe accepts arrays, but a single rxjs Subscription cannot map cleanly to a list of filters.
            throw new Error("subscribeResource does not support filter arrays");
        }

        const pool = await this.getPool(brokerUri, filter);

        const poolRef = pool;
        await pool.subscribe(
            filter,
            (_topic: string, message: Buffer, packet: mqtt.IPublishPacket) => {
                next(new Content(contentType, Readable.from(message), packet));

                if (
                    this.config.protocolVersion === 5 &&
                    packet.properties &&
                    packet.properties.responseTopic
                ) {
                    const options: mqtt.IClientPublishOptions = {
                        properties: {
                            userProperties: {
                                timestamp: new Date().toISOString(),
                            },
                        },
                    };
                    // Echo correlationData only if the requester sent one; per MQTT5 spec the
                    // response correlationData must equal the request's (or be absent).
                    if (packet.properties.correlationData) {
                        options.properties!.correlationData = packet.properties.correlationData;
                    }
                    poolRef
                        .publish(
                            packet.properties.responseTopic,
                            Buffer.from(JSON.stringify({ statusCode: 0, statusDesc: "OK" })),
                            options
                        )
                        .catch((err: Error) => {
                            warn(
                                `Failed to publish auto-ack on '${packet.properties!.responseTopic}': ${err.message}`
                            );
                        });
                }
            },
            (e: Error) => {
                if (error) error(e);
            }
        );

        return new Subscription(() => {
            poolRef.unsubscribe(filter as string).catch((e: Error) => {
                warn(`Failed to unsubscribe '${filter}': ${e.message}`);
            });
            if (complete) {
                try {
                    complete();
                } catch (e) {
                    warn(`subscribeResource complete callback threw: ${(e as Error).message}`);
                }
            }
        });
    }

    public async readResource(form: MqttForm): Promise<Content> {
        const contentType = form.contentType ?? ContentSerdes.DEFAULT;
        const requestUri = new url.URL(form.href);
        const brokerUri: string = `${this.scheme}://` + requestUri.host;
        // Keeping the path as the topic for compatibility reasons.
        // Current specification allows only form["mqv:filter"]. If href has no path (empty string),
        // `requestUri.pathname.slice(1)` returns '' which is not nullish — use explicit empty-check.
        const _path = requestUri.pathname.slice(1) || "";

        const filter = _path.length ? _path : form["mqv:filter"];

        const pool = await this.getPool(brokerUri, filter);

        const explicitResponseTopic = form["mqv:properties"] && form["mqv:properties"].responseTopic;
        // Default to adding '/response' if properties are missing and it's not a retained read
        const useDefaultResponseTopic =
            this.config.protocolVersion === 5 && !form["mqv:properties"];

        if (explicitResponseTopic || useDefaultResponseTopic) {
            const filterString = typeof filter === "string" ? filter : undefined;
            const responseTopic = explicitResponseTopic || (filterString ? `${filterString}/response` : undefined);

            if (!responseTopic) {
                throw new Error("No response topic could be determined");
            }

            const topic = _path.length ? _path : (form["mqv:topic"] || filterString);
            if (!topic) {
                throw new Error("No topic provided for read request");
            }

            const options: mqtt.IClientPublishOptions = {
                qos: mapQoS(form["mqv:qos"]),
                retain: form["mqv:retain"],
                properties: form["mqv:properties"] ?? {
                    responseTopic: responseTopic,
                },
            };

            return await this.publishAndWait(
                pool,
                topic,
                Buffer.from(""),
                options,
                responseTopic,
                contentType
            );
        }

        if (!filter) {
            throw new Error("No topic or filter provided");
        }

        let timer: NodeJS.Timeout;
        const poolRef = pool;
        const result = await new Promise<Content>((resolve, reject) => {
            timer = setTimeout(() => {
                poolRef.unsubscribe(filter).catch(() => {});
                reject(new Error(`Timeout waiting for message on topic ${filter}`));
            }, this.config.timeout ?? DEFAULT_TIMEOUT);

            poolRef.subscribe(
                filter,
                (_topic: string, message: Buffer, packet: mqtt.IPublishPacket) => {
                    clearTimeout(timer);
                    resolve(new Content(contentType, Readable.from(message), packet));
                },
                (e: Error) => {
                    clearTimeout(timer);
                    reject(e);
                }
            ).catch((e: Error) => {
                clearTimeout(timer);
                reject(e);
            });
        });

        await pool.unsubscribe(filter).catch(() => {});
        return result;
    }

    public async writeResource(form: MqttForm, content: Content): Promise<void | Content> {
        const requestUri = new url.URL(form.href);
        const brokerUri = `${this.scheme}://${requestUri.host}`;
        // `requestUri.pathname.slice(1)` may return an empty string when href contains only a host.
        // Fall back to form["mqv:topic"] in that case.
        const _path = requestUri.pathname.slice(1) || "";
        const topic = _path.length ? _path : form["mqv:topic"] ?? form["mqv:filter"];

        if (!topic) {
            throw new Error("No topic provided");
        }
        if (Array.isArray(topic)) {
            throw new Error("Topic cannot be an array");
        }

        const pool = await this.getPool(brokerUri, topic);

        // if not input was provided, set up an own body otherwise take input as body
        const buffer = content === undefined ? Buffer.from("") : await content.toBuffer();
        const options: mqtt.IClientPublishOptions = {
            retain: form["mqv:retain"],
            qos: mapQoS(form["mqv:qos"]),
        };

        // For v5: form-level mqv:properties wins; otherwise default to a request/response
        // pattern with `${topic}/response`. Callers that want fire-and-forget can set
        // mqv:properties without a responseTopic to opt out.
        if (this.config.protocolVersion === 5) {
            if (form["mqv:properties"]) {
                options.properties = { ...form["mqv:properties"] };
            } else {
                options.properties = { responseTopic: `${topic}/response` };
            }
        }

        if (content && content?.meta?.properties) {
            options.properties = { ...options.properties, ...content.meta.properties };
            if (content.meta.properties.userProperties) {
                options.properties!.userProperties = {
                    ...options.properties!.userProperties,
                    ...content.meta.properties.userProperties,
                };
            }
        }

        this.ensureV5Properties(options);

        if (options.properties && options.properties.responseTopic) {
            return await this.publishAndWait(
                pool,
                topic,
                buffer,
                options,
                options.properties.responseTopic,
                form.contentType ?? ContentSerdes.DEFAULT
            );
        }

        await pool.publish(topic, buffer, options);
    }

    public async invokeResource(form: MqttForm, content: Content): Promise<Content> {
        const requestUri = new url.URL(form.href);
        const brokerUri = `${this.scheme}://${requestUri.host}`;
        // `requestUri.pathname.slice(1)` may return an empty string when href contains only a host.
        // Fall back to form["mqv:topic"] in that case.
        const _path = requestUri.pathname.slice(1) || "";
        const topic = _path.length ? _path : form["mqv:topic"] ?? form["mqv:filter"];

        if (!topic) {
            throw new Error("No topic provided");
        }
        if (Array.isArray(topic)) {
            throw new Error("Topic cannot be an array");
        }

        const pool = await this.getPool(brokerUri, topic);

        // if not input was provided, set up an own body otherwise take input as body
        const buffer = content === undefined ? Buffer.from("") : await content.toBuffer();
        const options: mqtt.IClientPublishOptions = {
            retain: form["mqv:retain"],
            qos: mapQoS(form["mqv:qos"]),
        };

        // Same rationale as writeResource: form mqv:properties wins, else default to ${topic}/response.
        if (this.config.protocolVersion === 5) {
            if (form["mqv:properties"]) {
                options.properties = { ...form["mqv:properties"] };
            } else {
                options.properties = { responseTopic: `${topic}/response` };
            }
        }

        if (content && content.meta && content.meta.properties) {
            options.properties = { ...options.properties, ...content.meta.properties };
            if (content.meta.properties.userProperties) {
                options.properties!.userProperties = {
                    ...options.properties!.userProperties,
                    ...content.meta.properties.userProperties,
                };
            }
        }

        this.ensureV5Properties(options);

        if (options.properties && options.properties.responseTopic) {
            return await this.publishAndWait(
                pool,
                topic,
                buffer,
                options,
                options.properties.responseTopic,
                form.contentType ?? ContentSerdes.DEFAULT
            );
        }

        await pool.publish(topic, buffer, options);
        // there will be no response
        return new DefaultContent(Readable.from([]));
    }

    public async unlinkResource(form: Form): Promise<void> {
        const requestUri = new url.URL(form.href);
        const brokerUri: string = `${this.scheme}://` + requestUri.host;
        // `requestUri.pathname.slice(1)` may return an empty string when href contains only a host.
        // Fall back to form["mqv:filter"] in that case.
        const _path = requestUri.pathname.slice(1) || "";
        const topic = _path.length ? _path : (form as MqttForm)["mqv:filter"];

        if (!topic) {
            throw new Error("No topic or filter provided");
        }

        const pool = this.pools.get(this.shardKey(brokerUri, topic));
        if (pool != null) {
            await pool.unsubscribe(topic);
            debug(`MqttClient unsubscribed from topic '${topic}'`);
        }
    }

    /**
     * @inheritdoc
     */
    public async requestThingDescription(uri: string): Promise<Content> {
        throw new Error("Method not implemented");
    }

    public async start(): Promise<void> {
        // do nothing
    }

    public async stop(): Promise<void> {
        // Reject any in-flight requests before tearing down pools.
        for (const [pool, topicMap] of this.pendingByPool) {
            for (const reqMap of topicMap.values()) {
                for (const entry of reqMap.values()) {
                    if (entry.timer) clearTimeout(entry.timer);
                    entry.reject(new Error("MqttClient stopping"));
                }
            }
            topicMap.clear();
            this.pendingByPool.delete(pool);
        }
        for (const pool of this.pools.values()) {
            await pool.end();
        }
        if (this.client) return this.client.endAsync();
    }

    public setSecurity(metadata: Array<SecurityScheme>, credentials?: MqttClientSecurityParameters): boolean {
        if (metadata === undefined || !Array.isArray(metadata) || metadata.length === 0) {
            warn(`MqttClient received empty security metadata`);
            return false;
        }
        const security: SecurityScheme = metadata[0];

        if (security.scheme === "basic") {
            if (credentials === undefined) {
                // FIXME: This error message should be reworded and adapt to logging convention
                throw new Error("binding-mqtt: security wants to be basic but you have provided no credentials");
            } else {
                this.config.username = credentials.username;
                this.config.password = credentials.password;
            }
        }
        return true;
    }
}
