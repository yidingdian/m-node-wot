/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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
import { MqttClient, MqttForm } from "../src/mqtt";
import { Content } from "@node-wot/core";
import { Readable } from "stream";
import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import * as net from "net";
import * as mqttPacket from "mqtt-packet";

use(chaiAsPromised);

describe("MQTT client v5 support", () => {
    let server: net.Server;
    const brokerPort = 1891;
    const brokerUri = `mqtt://localhost:${brokerPort}`;
    let receivedPackets: mqttPacket.IPacket[] = [];
    let clientSocket: net.Socket | null = null;
    let client: MqttClient | undefined;

    const waitForPacket = async (cmd: string, timeout = 1000): Promise<mqttPacket.IPacket> => {
        const start = Date.now();
        while (Date.now() - start < timeout) {
            const packet = receivedPackets.find((p) => p.cmd === cmd);
            if (packet) return packet;
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        throw new Error(`Packet ${cmd} not found`);
    };

    beforeEach((done) => {
        receivedPackets = [];
        server = net.createServer((socket) => {
            clientSocket = socket;
            const parser = mqttPacket.parser({ protocolVersion: 5 });
            parser.on("packet", (packet) => {
                receivedPackets.push(packet);
                if (packet.cmd === "connect") {
                    const connack = mqttPacket.generate(
                        {
                            cmd: "connack",
                            reasonCode: 0,
                            sessionPresent: false,
                        },
                        { protocolVersion: 5 }
                    );
                    socket.write(connack);
                } else if (packet.cmd === "publish") {
                    if (packet.qos === 1) {
                        const puback = mqttPacket.generate(
                            {
                                cmd: "puback",
                                messageId: (packet as mqttPacket.IPublishPacket).messageId,
                                reasonCode: 0,
                            },
                            { protocolVersion: 5 }
                        );
                        socket.write(puback);
                    }
                } else if (packet.cmd === "subscribe") {
                    const suback = mqttPacket.generate(
                        {
                            cmd: "suback",
                            messageId: (packet as mqttPacket.ISubscribePacket).messageId,
                            granted: [0],
                        },
                        { protocolVersion: 5 }
                    );
                    socket.write(suback);
                } else if (packet.cmd === "unsubscribe") {
                    const unsuback = mqttPacket.generate(
                        {
                            cmd: "unsuback",
                            messageId: (packet as mqttPacket.IUnsubscribePacket).messageId,
                            granted: [0],
                        } as any,
                        { protocolVersion: 5 }
                    );
                    socket.write(unsuback);
                }
            });
            socket.on("data", (data) => {
                parser.parse(data);
            });
        });
        server.listen(brokerPort, done);
    });

    afterEach(async () => {
        if (clientSocket) clientSocket.destroy();
        if (client) client.stop();
        client = undefined;
        await new Promise<void>((resolve) => server.close(() => resolve()));
    });

    it("should pass properties when invoking a resource (MQTT v5)", async () => {
        client = new MqttClient({ protocolVersion: 5, reconnectPeriod: 0 } as any);
        const topic = "test/invoke";
        const form: MqttForm = {
            href: `${brokerUri}/${topic}`,
            "mqv:controlPacket": "publish",
        };
        const properties = {
            userProperties: {
                "test-prop": "test-value",
            },
            contentType: "application/json",
        };
        const content = new Content("application/json", Readable.from(Buffer.from("test")), { properties });

        await client.invokeResource(form, content);

        // Find the publish packet
        const publishPacket = (await waitForPacket("publish")) as mqttPacket.IPublishPacket;
        expect(publishPacket).to.exist;
        expect(publishPacket.properties).to.exist;
        expect(publishPacket.properties!.userProperties).to.deep.equal(properties.userProperties);
        expect(publishPacket.properties!.contentType).to.equal(properties.contentType);
    });

    it("should pass properties when writing a resource (MQTT v5)", async () => {
        client = new MqttClient({ protocolVersion: 5, reconnectPeriod: 0 } as any);
        const topic = "test/write";
        const form: MqttForm = {
            href: `${brokerUri}/${topic}`,
            "mqv:controlPacket": "publish",
        };
        const properties = {
            userProperties: {
                "test-prop": "test-value",
            },
        };
        const content = new Content("application/json", Readable.from(Buffer.from("test")), { properties });

        await client.writeResource(form, content);

        const publishPacket = (await waitForPacket("publish")) as mqttPacket.IPublishPacket;
        expect(publishPacket).to.exist;
        expect(publishPacket.properties).to.exist;
        expect(publishPacket.properties!.userProperties).to.deep.equal(properties.userProperties);
    });

    it("should expose properties in Content when subscribing (MQTT v5)", (done) => {
        client = new MqttClient({ protocolVersion: 5, reconnectPeriod: 0 } as any);
        const topic = "test/subscribe";
        const form: MqttForm = {
            href: `${brokerUri}/${topic}`,
            "mqv:controlPacket": "subscribe",
        };
        const properties = {
            userProperties: {
                "incoming-prop": "incoming-value",
            },
        };

        client
            .subscribeResource(form, (content) => {
                try {
                    expect(content.meta).to.exist;
                    expect(content.meta.properties).to.exist;
                    expect(content.meta.properties.userProperties).to.deep.equal(properties.userProperties);
                    done();
                } catch (e) {
                    done(e);
                }
            })
            .then(() => {
                // Wait for subscribe packet to be received by broker
                setTimeout(() => {
                    // Send a publish packet from broker to client
                    const publish = mqttPacket.generate(
                        {
                            cmd: "publish",
                            topic: topic,
                            payload: Buffer.from("test"),
                            qos: 0,
                            retain: false,
                            dup: false,
                            properties: properties,
                        },
                        { protocolVersion: 5 }
                    );
                    if (clientSocket) clientSocket.write(publish);
                }, 100);
            })
            .catch((e) => done(e));
    });

    it("should expose properties in Content when reading (MQTT v5)", async function () {
        this.timeout(5000);
        client = new MqttClient({ protocolVersion: 5, reconnectPeriod: 0 } as any);
        const topic = "test/read";
        const form: MqttForm = {
            href: `${brokerUri}/${topic}`,
            "mqv:controlPacket": "subscribe",
        };
        const properties = {
            userProperties: {
                "incoming-prop": "incoming-value",
            },
        };

        // For readResource, it subscribes, waits for message.
        // We need to send the message after subscription.
        // We can hook into the server to send message when subscribe is received.

        const originalListener = server.listeners("connection")[0] as (socket: net.Socket) => void;
        server.removeListener("connection", originalListener);

        server.on("connection", (socket) => {
            clientSocket = socket;
            const parser = mqttPacket.parser({ protocolVersion: 5 });
            parser.on("packet", (packet) => {
                receivedPackets.push(packet);
                if (packet.cmd === "connect") {
                    const connack = mqttPacket.generate(
                        {
                            cmd: "connack",
                            reasonCode: 0,
                            sessionPresent: false,
                        },
                        { protocolVersion: 5 }
                    );
                    socket.write(connack);
                } else if (packet.cmd === "subscribe") {
                    const suback = mqttPacket.generate(
                        {
                            cmd: "suback",
                            messageId: (packet as mqttPacket.ISubscribePacket).messageId,
                            granted: [0],
                        },
                        { protocolVersion: 5 }
                    );
                    socket.write(suback);

                    // Send the retained message immediately after suback
                    setTimeout(() => {
                        const publish = mqttPacket.generate(
                            {
                                cmd: "publish",
                                topic: topic,
                                payload: Buffer.from("test"),
                                qos: 0,
                                retain: true,
                                dup: false,
                                properties: properties,
                            },
                            { protocolVersion: 5 }
                        );
                        socket.write(publish);
                    }, 200);
                } else if (packet.cmd === "unsubscribe") {
                    const unsuback = mqttPacket.generate(
                        {
                            cmd: "unsuback",
                            messageId: (packet as mqttPacket.IUnsubscribePacket).messageId,
                            granted: [0],
                        } as any,
                        { protocolVersion: 5 }
                    );
                    socket.write(unsuback);
                }
            });
            socket.on("data", (data) => {
                parser.parse(data);
            });
        });

        const content = await client.readResource(form);

        expect(content.meta).to.exist;
        expect(content.meta.properties).to.exist;
        expect(content.meta.properties.userProperties).to.deep.equal(properties.userProperties);
    });
});
