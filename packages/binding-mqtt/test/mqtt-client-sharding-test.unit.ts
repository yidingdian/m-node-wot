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

// Tests for the per-device connection sharding added to MqttClient.
//   config.connectionShards>1 (or the MQTT_CONN_SHARDS env fallback) spreads the
//   per-device request/response load across N persistent connections, hashed by
//   the SN segment of the topic so a given device's request + responseTopic
//   subscribe always land on the same pool. The shard count is resolved in the
//   constructor, so tests inject it via config (the real configuration entry).

import { MqttClient, MqttForm } from "../src/mqtt";
import { expect } from "chai";
import * as net from "net";
import * as mqttPacket from "mqtt-packet";

describe("MqttClient connection sharding", () => {
    // ── Pure shardKey() contract — no network ───────────────────────────────
    describe("shardKey()", () => {
        const brokerUri = "mqtts://broker:8883";
        const newClient = (shards: number) =>
            new MqttClient({ protocolVersion: 5, connectionShards: shards } as never);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const shardKey = (c: MqttClient, uri: string, topic?: string | string[]) =>
            (c as any).shardKey(uri, topic) as string;

        it("returns the broker URI unchanged when shards<=1 (default off)", () => {
            expect(shardKey(newClient(1), brokerUri, "raft/light/SIM10_000001/properties/x/read")).to.equal(brokerUri);
        });

        it("is deterministic and stays within [0, shards)", () => {
            const c = newClient(8);
            const t = "raft/light/SIM10_000001/properties/genCfg/read";
            const k1 = shardKey(c, brokerUri, t);
            expect(shardKey(c, brokerUri, t)).to.equal(k1);
            expect(k1).to.match(/^mqtts:\/\/broker:8883#[0-7]$/);
        });

        it("keys by the SN segment, so different properties of one device map to the same shard", () => {
            const c = newClient(8);
            const read = shardKey(c, brokerUri, "raft/light/SIM10_000001/properties/genCfg/read");
            const action = shardKey(c, brokerUri, "raft/light/SIM10_000001/actions/reset");
            expect(action).to.equal(read);
        });

        it("maps distinct SNs across more than one shard, never exceeding shards", () => {
            const c = newClient(8);
            const seen = new Set<string>();
            for (let g = 0; g < 20; g++) {
                for (let d = 0; d < 20; d++) {
                    seen.add(shardKey(c, brokerUri, `raft/light/SIM${g}_${d}/properties/x/read`));
                }
            }
            expect(seen.size).to.be.greaterThan(1);
            expect(seen.size).to.be.at.most(8);
        });

        it("uses the first element when the topic is an array", () => {
            const c = newClient(8);
            const asArray = shardKey(c, brokerUri, ["raft/light/SIM10_000001/p", "raft/light/OTHER/p"]);
            const asSingle = shardKey(c, brokerUri, "raft/light/SIM10_000001/p");
            expect(asArray).to.equal(asSingle);
        });

        it("falls back to hashing the whole token when there is no SN segment", () => {
            const c = newClient(4);
            expect(shardKey(c, brokerUri, "shorttopic")).to.match(/^mqtts:\/\/broker:8883#[0-3]$/);
            expect(shardKey(c, brokerUri, undefined)).to.equal(`${brokerUri}#0`);
        });

        it("falls back to the MQTT_CONN_SHARDS env var when config omits connectionShards", () => {
            const prev = process.env.MQTT_CONN_SHARDS;
            process.env.MQTT_CONN_SHARDS = "4";
            try {
                const c = new MqttClient({ protocolVersion: 5 } as never);
                expect(shardKey(c, brokerUri, "raft/light/SIM10_000001/p")).to.match(
                    /^mqtts:\/\/broker:8883#[0-3]$/
                );
            } finally {
                if (prev === undefined) delete process.env.MQTT_CONN_SHARDS;
                else process.env.MQTT_CONN_SHARDS = prev;
            }
        });
    });

    // ── Behavioural fan-out against a fake MQTT5 broker ──────────────────────
    describe("connection fan-out (fake broker)", () => {
        const brokerPort = 1893;
        const brokerUri = `mqtt://localhost:${brokerPort}`;
        let server: net.Server;
        let sockets: { socket: net.Socket; packets: mqttPacket.IPacket[] }[];
        let client: MqttClient | undefined;

        const gen = (p: mqttPacket.IPacket) => mqttPacket.generate(p as never, { protocolVersion: 5 });

        const newClient = (shards: number) =>
            new MqttClient({ protocolVersion: 5, reconnectPeriod: 0, connectionShards: shards } as never);

        const handle = (socket: net.Socket, packet: mqttPacket.IPacket) => {
            if (packet.cmd === "connect") {
                socket.write(gen({ cmd: "connack", reasonCode: 0, sessionPresent: false } as never));
            } else if (packet.cmd === "subscribe") {
                socket.write(
                    gen({ cmd: "suback", messageId: (packet as mqttPacket.ISubscribePacket).messageId, granted: [0] } as never)
                );
            } else if (packet.cmd === "unsubscribe") {
                socket.write(
                    gen({ cmd: "unsuback", messageId: (packet as mqttPacket.IUnsubscribePacket).messageId, granted: [0] } as never)
                );
            } else if (packet.cmd === "publish") {
                const p = packet as mqttPacket.IPublishPacket;
                if (p.qos === 1) {
                    socket.write(gen({ cmd: "puback", messageId: p.messageId, reasonCode: 0 } as never));
                }
                // Mirror a broker: a request carries a responseTopic; reply on the
                // SAME socket (the one that subscribed it), echoing correlationData.
                const responseTopic = p.properties?.responseTopic;
                if (responseTopic && !p.topic.endsWith("/response")) {
                    setTimeout(() => {
                        if (socket.destroyed) return;
                        socket.write(
                            gen({
                                cmd: "publish",
                                topic: responseTopic,
                                payload: Buffer.from("OK"),
                                qos: 0,
                                retain: false,
                                dup: false,
                                properties: { correlationData: p.properties?.correlationData },
                            } as never)
                        );
                    }, 5);
                }
            }
        };

        beforeEach(async () => {
            sockets = [];
            server = await new Promise<net.Server>((resolve) => {
                const s = net.createServer((socket) => {
                    const rec = { socket, packets: [] as mqttPacket.IPacket[] };
                    sockets.push(rec);
                    const parser = mqttPacket.parser({ protocolVersion: 5 });
                    parser.on("packet", (packet) => {
                        rec.packets.push(packet);
                        handle(socket, packet);
                    });
                    socket.on("data", (d) => parser.parse(d));
                    socket.on("error", () => {
                        /* client teardown races — ignore */
                    });
                });
                s.listen(brokerPort, () => resolve(s));
            });
        });

        afterEach(async () => {
            if (client) await client.stop().catch(() => undefined);
            client = undefined;
            for (const r of sockets) r.socket.destroy();
            await new Promise<void>((resolve) => server.close(() => resolve()));
        });

        const readForm = (sn: string): { filter: string; form: MqttForm } => {
            const filter = `raft/light/${sn}/properties/genCfg/read`;
            return {
                filter,
                form: { href: `${brokerUri}/${filter}`, op: ["readproperty"], "mqv:filter": filter, "mqv:qos": "1" },
            };
        };

        // Mirrors MqttClient.shardKey's hash so we can pick SNs with known relative shards.
        const shardOf = (sn: string, shards: number): number => {
            const seg = `raft/light/${sn}/properties/genCfg/read`.split("/");
            const k = seg[2];
            let h = 0;
            for (let i = 0; i < k.length; i++) h = Math.imul(h, 31) + k.charCodeAt(i);
            return Math.abs(h | 0) % shards;
        };

        const socketOfRequest = (filter: string) =>
            sockets.filter((r) =>
                r.packets.some((p) => p.cmd === "publish" && (p as mqttPacket.IPublishPacket).topic === filter)
            );

        it("default (shards=1): distinct devices share a single connection", async () => {
            client = newClient(1);
            await client.readResource(readForm("SIM10_000001").form);
            await client.readResource(readForm("SIM77_000099").form);
            expect(sockets.length).to.equal(1);
        });

        it("shards>1: a device's request and its responseTopic subscribe land on the same connection", async () => {
            client = newClient(4);
            const { filter, form } = readForm("SIM10_000001");
            await client.readResource(form);

            const owning = sockets.filter(
                (r) =>
                    r.packets.some((p) => p.cmd === "publish" && (p as mqttPacket.IPublishPacket).topic === filter) &&
                    r.packets.some(
                        (p) =>
                            p.cmd === "subscribe" &&
                            (p as mqttPacket.ISubscribePacket).subscriptions[0].topic === `${filter}/response`
                    )
            );
            expect(owning.length).to.equal(1);
        });

        it("shards>1: the same SN reuses its connection (no new socket on a second read)", async () => {
            client = newClient(8);
            await client.readResource(readForm("SIM10_000001").form);
            const afterFirst = sockets.length;
            await client.readResource(readForm("SIM10_000001").form);
            expect(sockets.length).to.equal(afterFirst);
        });

        it("shards>1: SNs on different shards open separate connections, same-shard SNs share one", async () => {
            const SHARDS = 8;
            client = newClient(SHARDS);

            // Pick A, then B on a different shard, then C sharing A's shard.
            const cand = (i: number) => `SIM${i}_${i}`;
            const a = cand(1);
            const sa = shardOf(a, SHARDS);
            let b: string | undefined;
            let c: string | undefined;
            for (let i = 2; i < 500 && (!b || !c); i++) {
                const s = shardOf(cand(i), SHARDS);
                if (!b && s !== sa) b = cand(i);
                else if (!c && s === sa) c = cand(i);
            }
            expect(b, "need an SN on a different shard").to.be.a("string");
            expect(c, "need an SN on the same shard as A").to.be.a("string");

            await client.readResource(readForm(a).form);
            await client.readResource(readForm(b!).form);
            await client.readResource(readForm(c!).form);

            // A and C collapse onto one connection; B is separate → 2 total.
            expect(sockets.length).to.equal(2);
            const sockA = socketOfRequest(readForm(a).filter)[0];
            const sockB = socketOfRequest(readForm(b!).filter)[0];
            const sockC = socketOfRequest(readForm(c!).filter)[0];
            expect(sockA).to.equal(sockC);
            expect(sockB).to.not.equal(sockA);
        });

        it("shards>1: many distinct devices fan out within [2, shards] connections", async function () {
            this.timeout(15000);
            const SHARDS = 4;
            client = newClient(SHARDS);
            for (let g = 0; g < 6; g++) {
                for (let d = 0; d < 4; d++) {
                    await client.readResource(readForm(`SIM${g}_${d}`).form);
                }
            }
            expect(sockets.length).to.be.greaterThan(1);
            expect(sockets.length).to.be.at.most(SHARDS);
        });
    });
});
