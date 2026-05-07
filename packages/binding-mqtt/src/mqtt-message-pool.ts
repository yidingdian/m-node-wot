/********************************************************************************
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
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
import { createLoggers } from "@node-wot/core";
import { MqttClientConfig } from "./mqtt";
import * as mqtt from "mqtt";

const { debug, warn } = createLoggers("binding-mqtt", "mqtt-message-pool");

export default class MQTTMessagePool {
    client?: mqtt.MqttClient;
    subscribers: Map<string, (topic: string, message: Buffer, packet: mqtt.IPublishPacket) => void> = new Map();
    errors: Map<string, (error: Error) => void> = new Map();
    private connectionPromise?: Promise<void>;

    public async connect(brokerURI: string, config: MqttClientConfig): Promise<void> {
        if (this.client) {
            return;
        }
        if (this.connectionPromise) {
            // 有其他并发connect正在进行，等待其完成
            await this.connectionPromise;
            return;
        }
        this.connectionPromise = (async () => {
            this.client = await mqtt.connectAsync(brokerURI, config);
            this.client.on("message", (receivedTopic: string, payload: Buffer, packet: mqtt.IPublishPacket) => {
                debug(
                    `Received MQTT message from ${brokerURI} (topic: ${receivedTopic}, data length: ${payload.length})`,
                    packet?.properties
                );
                this.subscribers.get(receivedTopic)?.(receivedTopic, payload, packet);
            });
            // Connection errors should be deal by the connectAsync
            // here we handle "runtime" parsing errors, but we can't do much
            // therefore we broadcast the error to all subscribers
            this.client.on("error", (error: Error) => {
                warn(`MQTT client error: ${error.message}`);
                this.errors.forEach((errorCallback) => {
                    errorCallback(error);
                });
            });
            // After a reconnect with no persistent session the broker has dropped our subscriptions.
            // Re-issue them so consumers don't go silent. The library re-emits 'connect' on every
            // (re)connect; on the first connect this is a no-op because subscribers is still empty.
            this.client.on("connect", (connack: mqtt.IConnackPacket) => {
                if (connack && (connack as { sessionPresent?: boolean }).sessionPresent) {
                    return;
                }
                const filters = Array.from(this.subscribers.keys());
                if (filters.length === 0) return;
                this.client!.subscribeAsync(filters).catch((err: Error) => {
                    warn(`Failed to re-subscribe ${filters.length} filters after reconnect: ${err.message}`);
                });
            });
        })();
        try {
            await this.connectionPromise;
        } finally {
            this.connectionPromise = undefined;
        }
    }

    public async subscribe(
        filter: string | string[],
        callback: (topic: string, message: Buffer, packet: mqtt.IPublishPacket) => void,
        error: (error: Error) => void
    ): Promise<void> {
        if (this.client == null) {
            throw new Error("MQTT client is not connected");
        }

        const filters = Array.isArray(filter) ? filter : [filter];
        const toSubscribe: string[] = [];
        filters.forEach((f) => {
            if (this.subscribers.has(f)) {
                warn(`Already subscribed to ${f}; we are not supporting multiple subscribers to the same topic`);
                warn(`The subscription will be ignored`);
                return;
            }

            this.subscribers.set(f, callback);
            this.errors.set(f, error);
            toSubscribe.push(f);
        });

        if (toSubscribe.length === 0) return;
        try {
            await this.client.subscribeAsync(toSubscribe);
        } catch (err) {
            // Roll back registry on broker-side failure so the caller can retry / surface the error.
            toSubscribe.forEach((f) => {
                this.subscribers.delete(f);
                this.errors.delete(f);
            });
            throw err;
        }
    }

    public async unsubscribe(filter: string | string[]): Promise<void> {
        if (this.client == null) {
            throw new Error("MQTT client is not connected");
        }

        const filters = Array.isArray(filter) ? filter : [filter];
        filters.forEach((f) => {
            this.subscribers.delete(f);
            this.errors.delete(f);
        });

        await this.client.unsubscribeAsync(filters);
    }

    public async publish(topic: string, message: Buffer, options?: mqtt.IClientPublishOptions): Promise<void> {
        if (this.client == null) {
            throw new Error("MQTT client is not connected");
        }

        debug(`Publishing MQTT message to ${topic} (data length: ${message.length})`);
        await this.client.publishAsync(topic, message, options);
    }

    public async end(): Promise<void> {
        const filters = Array.from(this.subscribers.keys());
        for (const filter of filters) {
            try {
                await this.unsubscribe(filter);
            } catch (e) {
                // ignore — we're tearing down anyway
            }
        }
        return this.client?.endAsync();
    }
}
