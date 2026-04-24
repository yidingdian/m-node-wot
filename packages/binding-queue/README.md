# Queue Binding for node-wot

Queue-based protocol binding for [Web of Things (WoT)](https://www.w3.org/WoT/), using FairQueue (Redis-based) for asynchronous, fair-scheduled communication between Thing servers and clients.

## Features

- **Per-Device Fair Scheduling**: Round-robin active device ring with per-device FIFO execution
- **Device-Level Isolation**: Each device has its own Redis ZSET + execution lock, eliminating cross-device blocking
- **Gateway Rate Limiting**: Lua-based sliding time window limiter per gateway
- **Stalled Device Recovery**: Watchdog automatically detects and recovers crashed device locks
- **Dedup/Override**: Memory-buffered writes with timestamp-based dedup for high-frequency property updates
- **Request-Response**: Synchronous `waitJobDone` with Pub/Sub-based result delivery (zero polling)
- **WoT Compliant**: Implements standard WoT `ProtocolClient` and `ProtocolServer` interfaces

## Architecture

```
┌──────────────────────┐                          ┌──────────────────────┐
│       Client         │                          │       Server         │
│                      │                          │                      │
│  QueueClientFactory  │                          │  QueueProtocolServer │
│  QueueProtocolClient │                          │                      │
│    ├─ sendQ (producer)│    Redis FairQueue       │    ├─ sendQ (consumer)│
│    │  waitJobDone()  │───── sendQ ──────────────▶│    │  processCommand()│
│    │  queueJob()     │                          │    │                  │
│    │                 │                          │    │  ExposedThing    │
│    │                 │                          │    │  handlers        │
│    │                 │                          │    │                  │
│    └─ recvQ (consumer)│◀──── recvQ ─────────────│    └─ recvQ (producer)│
│       processEvent() │                          │       emitEvent()    │
│       subscribers    │                          │       emitProperty() │
└──────────────────────┘                          └──────────────────────┘
```

## Installation

```bash
npm install @yidingdian/binding-queue
```

## Prerequisites

- Redis server running (default: localhost:6379)
- Node.js 18+
- FairQueue implementation (injected via `FairQueueClass`)

## Quick Start

### Server Side - Exposing Things

```javascript
const { Servient } = require('@yidingdian/core');
const { QueueProtocolServer } = require('@yidingdian/binding-queue');

const servient = new Servient();

const queueServer = new QueueProtocolServer({
    FairQueueClass: FairQueue,
    redisOptions: { host: 'localhost', port: 6379, db: 2 },
    sendQueueName: 'sendQ',
    recvQueueName: 'recvQ',
    maxConcurrency: 5000,
    waitTTLMs: 6000,
    lockTTLMs: 30000,
    gatewayRateLimit: {
        enabled: true,
        defaultLimit: 4,
        resolver: (jobData) => jobData.parentSn || null,
    },
});

servient.addServer(queueServer);

const wot = await servient.start();

const thing = await wot.produce({
    title: 'MyDevice',
    properties: {
        brightness: { type: 'integer', observable: true },
    },
    actions: {
        toggle: { input: { type: 'object' }, output: { type: 'object' } },
    },
    events: {
        statusChanged: { data: { type: 'string' } },
    },
});

// Handlers are called when the server processes commands from sendQ
thing.setPropertyReadHandler('brightness', async () => {
    return await readFromDevice(thing.id, 'brightness');
});

thing.setActionHandler('toggle', async (params) => {
    const input = await params.value();
    return await invokeOnDevice(thing.id, 'toggle', input);
});

await thing.expose();
```

### Client Side - Consuming Things

```javascript
const { Servient } = require('@yidingdian/core');
const { QueueClientFactory } = require('@yidingdian/binding-queue');

const servient = new Servient();

const factory = new QueueClientFactory({
    FairQueueClass: FairQueue,
    redisOptions: { host: 'localhost', port: 6379, db: 2 },
    sendQueueName: 'sendQ',
    recvQueueName: 'recvQ',
    snResolver: async (thingId) => {
        const device = await db.findByThingId(thingId);
        return device.sn;
    },
    parentSnResolver: async (thingId) => {
        const device = await db.findByThingId(thingId);
        return device.parentSn;
    },
});

servient.addClientFactory(factory);

const wot = await servient.start();
const thing = await wot.consume(td);

// Read property (synchronous via sendQ.waitJobDone)
const brightness = await thing.readProperty('brightness');
console.log(await brightness.value());

// Write property (buffered via sendQ.queueJob, dedup by jobId)
await thing.writeProperty('brightness', 80);

// Invoke action (synchronous via sendQ.waitJobDone)
const result = await thing.invokeAction('toggle', { state: true });
console.log(await result.value());

// Subscribe to events (received via recvQ consumer)
await thing.subscribeEvent('statusChanged', async (data) => {
    console.log('Status changed:', await data.value());
});
```

## Configuration

### QueueConfig (shared)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `redisOptions` | `RedisConnectionOptions` | `{ host: 'localhost', port: 6379 }` | ioredis connection options |
| `sendQueueName` | `string` | `"sendQ"` | FairQueue name for downlink commands |
| `recvQueueName` | `string` | `"recvQ"` | FairQueue name for uplink events |
| `maxConcurrency` | `number` | `5000` | Max parallel device execution |
| `waitTTLMs` | `number` | `6000` | Default timeout for waitJobDone |
| `lockTTLMs` | `number` | `30000` | Per-device lock TTL |
| `logger` | `QueueLogger` | `debug('binding-queue')` | bunyan-compatible logger |

### QueueProtocolServerConfig (extends QueueConfig)

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `FairQueueClass` | `class` | ✅ | FairQueue constructor (injected) |
| `gatewayRateLimit` | `GatewayRateLimitConfig` | ❌ | Gateway rate limiting config |

### QueueProtocolClientConfig (extends QueueConfig)

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `FairQueueClass` | `class` | ✅ | FairQueue constructor (injected) |
| `snResolver` | `(thingId, name) => string` | ❌ | Resolve device SN from thingId |
| `parentSnResolver` | `(thingId) => string?` | ❌ | Resolve gateway SN for rate limiting |

## Queue URI Scheme

```
queue://{thingTitle}/{interactionType}/{interactionName}
```

Examples:
- `queue://MyDevice/actions/toggle`
- `queue://MyDevice/properties/brightness`
- `queue://MyDevice/events/statusChanged`

## FairQueue Data Flow

### Downlink (Read/Action - synchronous)

```
Client.readResource(form)
  → sendQ.waitJobDone(sn, "readProperty", { thingId, name, ... })
  → Redis: fq:{sn}:jobs ZADD + fq:active RPUSH
  → Server scheduler: BLPOP fq:active → dequeue → lock
  → processCommand(job) → ExposedThing.handleReadProperty()
  → MQTT read from real device → result
  → Redis SET fq:result:{jobId} + PUBLISH
  → Client Pub/Sub → resolve → Content
```

### Downlink (Write - buffered)

```
Client.writeResource(form, content)
  → sendQ.queueJob(sn, "writeProperty", { thingId, name, data, ... })
  → Memory buffer (200ms dedup by jobId)
  → Flush → Redis enqueue
  → Server processes as above
```

### Uplink (Event/Property)

```
Real Device → MQTT → consumer callback
  → emitEvent() → recvQ.add("event", payload)
  → Redis: fq:{sn}:jobs ZADD
  → Client recvQ scheduler → processEvent(job)
  → Dispatch to subscribers → Content callback
```

## Migration from BullMQ binding

| Before (BullMQ) | After (FairQueue) |
|------------------|-------------------|
| `bullmq` dependency | FairQueue (injected via `FairQueueClass`) |
| `QueueConfig.redis` (ConnectionOptions) | `QueueConfig.redisOptions` (ioredis options) |
| `QueueConfig.queueName` (single prefix) | `sendQueueName` + `recvQueueName` |
| `QueueConfig.requestTimeout` | `QueueConfig.waitTTLMs` |
| `QueueProtocolServerConfig.concurrency` | `QueueConfig.maxConcurrency` |
| Single command/response queue pair | Two FairQueue instances (sendQ/recvQ) |
| correlationId-based request matching | FairQueue Pub/Sub result delivery |
| No device isolation | Per-device ZSET + lock isolation |
| No rate limiting | Gateway-level sliding window |
| No stalled recovery | Automatic watchdog |

## Development

```bash
# Build
npm run build

# Test (requires Redis + FairQueue)
npm test

# Clean
npm run clean
```

## License

EPL-2.0 OR W3C-20150513
