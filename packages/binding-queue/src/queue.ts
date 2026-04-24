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
 * @module binding-queue
 *
 * Queue binding types and utilities for node-wot.
 *
 * This binding uses FairQueue as the underlying queue engine,
 * providing per-device fair scheduling, gateway-level rate limiting, and
 * device-level isolation via Redis.
 *
 * Data Flow:
 *   Downlink (client→server): Client.invokeResource() → sendQ.waitJobDone()
 *       → Server FairQueue processor → ExposedThing handler → result via Pub/Sub
 *   Uplink (server→client): Server.emitEvent() → recvQ.add()
 *       → Client FairQueue processor → subscriber callback
 */

import { Form } from "@yidingdian/core";

// ─── Redis Configuration ────────────────────────────────────────────────────

/**
 * Redis connection options compatible with ioredis.
 * Subset of ioredis.RedisOptions used by FairQueue.
 */
export interface RedisConnectionOptions {
    host?: string;
    port?: number;
    password?: string | null;
    db?: number;
    /** Must be null for BullMQ/FairQueue compatibility */
    maxRetriesPerRequest?: number | null;
    /** TLS options */
    tls?: Record<string, unknown>;
    /** Key prefix */
    keyPrefix?: string;
    [key: string]: unknown;
}

// ─── Queue Configuration ────────────────────────────────────────────────────

/**
 * Gateway-level rate limiting configuration.
 *
 * When enabled, limits the number of concurrent jobs per gateway within
 * a sliding time window (1 second), preventing any single gateway from
 * monopolizing queue resources.
 */
export interface GatewayRateLimitConfig {
    /** Enable gateway-level rate limiting (default: false) */
    enabled: boolean;
    /** Default max requests per second per gateway. 0 = disabled (default: 0) */
    defaultLimit?: number;
    /**
     * Custom resolver to extract gateway ID from job data/options.
     * If not provided, falls back to common field names (parentSn, gatewayId, gwSn, etc.)
     */
    resolver?: (jobData: Record<string, unknown>, jobOpts: Record<string, unknown>) => string | null;
}

/**
 * Queue binding configuration.
 *
 * Maps directly to FairQueue constructor config.
 * The binding creates two FairQueue instances:
 *   - sendQ (downlink): commands from client to server
 *   - recvQ (uplink): events/properties from server to client
 */
export interface QueueConfig {
    /** Redis connection options (ioredis compatible) */
    redisOptions?: RedisConnectionOptions;
    /**
     * Queue name prefix for the send (downlink/command) queue.
     * Used as FairQueue's queueName parameter for the command channel.
     * Default: "sendQ"
     */
    sendQueueName?: string;
    /**
     * Queue name prefix for the receive (uplink/event) queue.
     * Used as FairQueue's queueName parameter for the event channel.
     * Default: "recvQ"
     */
    recvQueueName?: string;
    /**
     * Maximum parallel job execution (FairQueue maxConcurrency).
     * Controls how many devices can be processed simultaneously.
     * Default: 5000
     */
    maxConcurrency?: number;
    /**
     * Default timeout for waitJobDone in milliseconds.
     * Default: 6000
     */
    waitTTLMs?: number;
    /**
     * Per-device lock TTL in milliseconds.
     * Must be greater than the slowest expected job processing time.
     * Default: 30000
     */
    lockTTLMs?: number;
    /**
     * Gateway-level rate limiting configuration.
     * Only applicable to the send (downlink) queue.
     */
    gatewayRateLimit?: GatewayRateLimitConfig;
    /**
     * External logger instance (bunyan-compatible).
     * If not provided, uses debug('binding-queue').
     */
    logger?: QueueLogger;
}

/**
 * Logger interface compatible with bunyan/debug loggers.
 */
export interface QueueLogger {
    info: (...args: unknown[]) => void;
    warn: (...args: unknown[]) => void;
    error: (...args: unknown[]) => void;
    debug: (...args: unknown[]) => void;
    trace: (...args: unknown[]) => void;
}

// ─── Form Extension ─────────────────────────────────────────────────────────

/**
 * Queue-specific WoT form extension.
 *
 * These fields are embedded in Thing Description forms to control
 * per-interaction queue behavior.
 */
export interface QueueForm extends Form {
    /** Number of retry attempts within the device lock hold (default: 1) */
    "queue:retries"?: number;
    /** Job priority: 1=highest ... 9=lowest (default: 5) */
    "queue:priority"?: number;
    /** Job delay in milliseconds before becoming eligible for processing */
    "queue:delay"?: number;
    /** Job timeout in milliseconds for waitJobDone (overrides global waitTTLMs) */
    "queue:timeout"?: number;
}

// ─── Request / Response Types ───────────────────────────────────────────────

/**
 * WoT operation types that can be sent as queue commands.
 */
export type QueueRequestType =
    | "invokeAction"
    | "readProperty"
    | "writeProperty"
    | "observeProperty"
    | "unobserveProperty"
    | "subscribeEvent"
    | "unsubscribeEvent"
    | "getTd";

/**
 * FairQueue job data structure for downlink commands.
 *
 * This is the payload stored in Redis (fq:{sn}:data:{jobId}) for command jobs.
 * The server's FairQueue processor receives this as `job.data`.
 */
export interface QueueCommandJobData {
    /** Type of WoT operation */
    type: QueueRequestType;
    /** Thing ID (full URN, e.g. "urn:dev:wot:gateway-001:light-01") */
    thingId: string;
    /** Device SN used as the FairQueue routing key */
    sn: string;
    /** Parent device SN (gateway SN) for rate limiting */
    parentSn?: string;
    /** Interaction name (action/property/event name) */
    name: string;
    /** Input data for the operation */
    data?: unknown;
    /** Timestamp for dedup/override ordering */
    timestamp: number;
}

/**
 * FairQueue job data structure for uplink events/property changes.
 *
 * This is the payload stored in the recvQ for event jobs.
 * The client's FairQueue processor receives this as `job.data`.
 */
export interface QueueEventJobData {
    /** Thing title (human-readable name) */
    title: string;
    /** Thing ID (full URN) */
    thingId: string;
    /** Event or property name */
    name: string;
    /** Event/property data payload */
    data: unknown;
    /** Timestamp for ordering */
    timestamp: number;
}

/**
 * FairQueue job data structure for TD notifications (recvQ).
 *
 * Pushed from server (raft-cloud) when a shadow TD is created, updated, or deleted.
 * lightingMain's msg.js receives these to maintain a local TD cache, replacing
 * the HTTP-based requestThing() + SSE notification path.
 */
export interface QueueTdJobData {
    /** Thing title (shadow TD title, used as productId) */
    title: string;
    /** Thing ID (full URN) */
    thingId: string;
    /** Device SN (FairQueue routing key) */
    sn: string;
    /** Full Thing Description (empty object for deletions) */
    td: Record<string, unknown>;
    /** Timestamp */
    timestamp: number;
    /** Operation that triggered this notification */
    operation: "thing_created" | "thing_updated" | "thing_deleted";
    /** Shadow device metadata for status sync on startup */
    shadowDevice?: {
        status?: string;
        parentSn?: string;
    };
}

// ─── URI Scheme Utilities ───────────────────────────────────────────────────

/**
 * Parse a queue:// URI into components.
 *
 * Required URI format: queue://{title}/{thingId}/{interactionType}/{interactionName}
 *
 * Examples:
 *   queue://my-light/urn%3Adev%3Awot%3Agw-1%3Alight-01/actions/toggle
 *     → { title: "my-light", thingId: "urn:dev:wot:gw-1:light-01", interactionType: "actions", interactionName: "toggle" }
 */
export function parseQueueUri(href: string): {
    queueName: string;
    title: string;
    thingId: string;
    interactionType: "actions" | "properties" | "events" | null;
    interactionName: string | null;
} {
    const url = new URL(href);
    const pathParts = url.pathname.split("/").filter((p) => p.length > 0);
    const title = url.hostname;

    if (pathParts.length < 3) {
        throw new Error(
            `Invalid queue URI '${href}'. Expected format: queue://{title}/{thingId}/{interactionType}/{interactionName}`
        );
    }

    const type = pathParts[1];
    if (type !== "actions" && type !== "properties" && type !== "events") {
        throw new Error(
            `Invalid queue URI '${href}'. interactionType must be one of: actions, properties, events`
        );
    }

    let thingId: string;
    try {
        thingId = decodeURIComponent(pathParts[0]);
    } catch {
        throw new Error(`Invalid queue URI '${href}'. thingId segment is not valid URL encoding`);
    }

    const interactionType = type;
    const interactionName = pathParts[2];

    return {
        queueName: title,
        title,
        thingId,
        interactionType,
        interactionName,
    };
}

/**
 * Build a queue:// URI from components using the preferred format:
 *   queue://{title}/{encodeURIComponent(thingId)}/{interactionType}/{interactionName}
 */
export function buildQueueUri(
    title: string,
    thingId: string,
    interactionType: "actions" | "properties" | "events",
    interactionName: string
): string {
    return `queue://${title}/${encodeURIComponent(thingId)}/${interactionType}/${interactionName}`;
}

// ─── FairQueue Type Declarations ────────────────────────────────────────────

/**
 * FairQueue job structure as received by the processor callback.
 * Matches the shape from the FairQueue processor callback.
 */
export interface FairQueueJob {
    id: string;
    name: string;
    sn: string;
    data: Record<string, unknown>;
    opts: Record<string, unknown>;
    timestamp: number;
    attemptsMade?: number;
    _waitResult?: boolean;
    gatewayId?: string;
}

/**
 * FairQueue instance interface (duck-typed).
 *
 * This allows the binding to work with the FairQueue class without
 * requiring a direct TypeScript dependency on the queue library.
 */
export interface FairQueueInstance {
    /** Fire-and-forget enqueue */
    addJob(
        sn: string,
        jobName: string,
        jobData: Record<string, unknown>,
        jobOpts?: Record<string, unknown>
    ): Promise<{ jobId: string }>;

    /** Enqueue and wait for result via Pub/Sub */
    waitJobDone(
        sn: string,
        jobName: string,
        jobData: Record<string, unknown>,
        jobOpts?: Record<string, unknown>
    ): Promise<unknown>;

    /** Override/dedup enqueue with timestamp comparison */
    overrideJob(
        sn: string,
        jobName: string,
        jobData: Record<string, unknown>,
        jobOpts?: Record<string, unknown>
    ): Promise<unknown>;

    /** Memory-buffered enqueue (flush every 200ms, dedup by jobId) */
    queueJob(
        sn: string,
        jobName: string,
        jobData: Record<string, unknown>,
        jobOpts?: Record<string, unknown>
    ): Promise<{ queued: boolean; replaced?: boolean; skipped?: boolean }>;

    /** BullMQ-compatible add (extracts sn from data.sn/thingId/deviceId) */
    add(
        name: string,
        data: Record<string, unknown>,
        opts?: Record<string, unknown>
    ): Promise<{ jobId: string }>;

    /** Check if a job exists in a device queue */
    getQJob(
        sn: string,
        parentSn: string | undefined,
        jobId: string
    ): Promise<FairQueueJob | null>;

    /** Remove a specific job */
    removeJob(sn: string, jobId: string): Promise<void>;

    /** Clear all jobs for a device */
    clearQueue(sn: string): Promise<void>;

    /** Stop the scheduler and close Redis connections */
    stop(): Promise<void>;

    /** Health check */
    health(): Promise<{ ok: boolean; active: string[] }>;
}
