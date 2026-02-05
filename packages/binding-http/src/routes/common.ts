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
import { ContentSerdes, ExposedThing, Helpers, createLoggers } from "@node-wot/core";
import { IncomingMessage, ServerResponse } from "http";

const { debug, warn } = createLoggers("binding-http", "routes", "common");

/**
 * Error to HTTP status code mapping result
 */
export interface HttpErrorResponse {
    statusCode: number;
    statusMessage: string;
    message: string;
}

/**
 * Map an error to appropriate HTTP status code and message
 *
 * This function analyzes the error message/type and returns
 * the most appropriate HTTP status code instead of always returning 500.
 *
 * @param err - The error to map
 * @returns HttpErrorResponse with status code, status message, and error message
 */
export function mapErrorToHttpResponse(err: unknown): HttpErrorResponse {
    const message = err instanceof Error ? err.message : JSON.stringify(err);
    const lowerMessage = message.toLowerCase();

    // Timeout errors -> 504 Gateway Timeout
    if (lowerMessage.includes("timeout") || lowerMessage.includes("timed out")) {
        return {
            statusCode: 504,
            statusMessage: "Timeout",
            message: message,
        };
    }

    // Connection refused/failed -> 502 Bad Gateway
    if (
        lowerMessage.includes("econnrefused") ||
        lowerMessage.includes("connection refused") ||
        lowerMessage.includes("connection failed") ||
        lowerMessage.includes("econnreset")
    ) {
        return {
            statusCode: 502,
            statusMessage: "Bad Gateway",
            message: message,
        };
    }

    // Not found errors -> 404 Not Found
    if (lowerMessage.includes("not found") || lowerMessage.includes("does not exist")) {
        return {
            statusCode: 404,
            statusMessage: "Not Found",
            message: message,
        };
    }

    // Invalid input/validation errors -> 400 Bad Request
    if (
        lowerMessage.includes("invalid") ||
        lowerMessage.includes("validation") ||
        lowerMessage.includes("malformed") ||
        lowerMessage.includes("bad request")
    ) {
        return {
            statusCode: 400,
            statusMessage: "Bad Request",
            message: message,
        };
    }

    // Unauthorized/authentication errors -> 401 Unauthorized
    if (
        lowerMessage.includes("unauthorized") ||
        lowerMessage.includes("authentication") ||
        lowerMessage.includes("not authenticated")
    ) {
        return {
            statusCode: 401,
            statusMessage: "Unauthorized",
            message: message,
        };
    }

    // Forbidden/permission errors -> 403 Forbidden
    if (
        lowerMessage.includes("forbidden") ||
        lowerMessage.includes("permission denied") ||
        lowerMessage.includes("access denied")
    ) {
        return {
            statusCode: 403,
            statusMessage: "Forbidden",
            message: message,
        };
    }

    // Service unavailable -> 503 Service Unavailable
    if (
        lowerMessage.includes("unavailable") ||
        lowerMessage.includes("service unavailable") ||
        lowerMessage.includes("offline")
    ) {
        return {
            statusCode: 503,
            statusMessage: "Service Unavailable",
            message: message,
        };
    }

    // Conflict errors -> 409 Conflict
    if (lowerMessage.includes("conflict") || lowerMessage.includes("already exists")) {
        return {
            statusCode: 409,
            statusMessage: "Conflict",
            message: message,
        };
    }

    // Default: 500 Internal Server Error
    return {
        statusCode: 500,
        statusMessage: "Internal Server Error",
        message: message,
    };
}

export function respondUnallowedMethod(
    req: IncomingMessage,
    res: ServerResponse,
    allowed: string,
    corsPreflightWithCredentials = false
): void {
    // Always allow OPTIONS to handle CORS pre-flight requests
    if (!allowed.includes("OPTIONS")) {
        allowed += ", OPTIONS";
    }

    const headers = req.headers;
    const origin = headers.origin;
    if (req.method === "OPTIONS" && origin != null && headers["access-control-request-method"] != null) {
        debug(
            `HttpServer received an CORS preflight request from ${Helpers.toUriLiteral(req.socket.remoteAddress)}:${
                req.socket.remotePort
            }`
        );
        if (corsPreflightWithCredentials) {
            res.setHeader("Access-Control-Allow-Origin", origin);
            res.setHeader("Access-Control-Allow-Credentials", "true");
        } else {
            res.setHeader("Access-Control-Allow-Origin", "*");
        }
        res.setHeader("Access-Control-Allow-Methods", allowed);
        res.setHeader("Access-Control-Allow-Headers", "content-type, authorization, *");
        res.writeHead(200);
        res.end();
    } else {
        res.setHeader("Allow", allowed);
        res.writeHead(405);
        res.end("Method Not Allowed");
    }
}

export function validOrDefaultRequestContentType(
    req: IncomingMessage,
    res: ServerResponse,
    contentType: string
): string {
    if (req.method === "PUT" || req.method === "POST") {
        if (!contentType) {
            // FIXME should be rejected with 400 Bad Request, as guessing is not good in M2M -> debug/testing flag to allow
            // FIXME would need to check if payload is present
            warn(
                `HttpServer received no Content-Type from ${Helpers.toUriLiteral(req.socket.remoteAddress)}:${
                    req.socket.remotePort
                }`
            );
            return ContentSerdes.DEFAULT;
        } else if (ContentSerdes.get().getSupportedMediaTypes().indexOf(ContentSerdes.getMediaType(contentType)) < 0) {
            throw new Error("Unsupported Media Type");
        }
        return contentType;
    }
    return contentType;
}

export function isEmpty(obj: Record<string, unknown>): boolean {
    for (const key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key)) return false;
    }
    return true;
}

export function securitySchemeToHttpHeader(scheme: string): string {
    const [first, ...rest] = scheme;
    // HTTP Authentication Scheme for OAuth does not contain the version number
    // see https://www.iana.org/assignments/http-authschemes/http-authschemes.xhtml
    if (scheme === "oauth2") return "OAuth";
    return first.toUpperCase() + rest.join("").toLowerCase();
}

export function setCorsForThing(req: IncomingMessage, res: ServerResponse, thing: ExposedThing): void {
    const securityScheme = thing.securityDefinitions[Helpers.toStringArray(thing.security)[0]].scheme;
    // Set CORS headers

    const origin = req.headers.origin;
    if (securityScheme !== "nosec" && origin != null) {
        res.setHeader("Access-Control-Allow-Origin", origin);
        res.setHeader("Access-Control-Allow-Credentials", "true");
    } else {
        res.setHeader("Access-Control-Allow-Origin", "*");
    }
}
