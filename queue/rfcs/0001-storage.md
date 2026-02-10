# RFC 0001: Queue Storage

**Status**: Draft

**Authors**:
- [Bruno Cadonna](https://github.com/cadonna>)

## Summary

This RFC defines the storage format for OpenData-Queue.
The design separates message storage from queue state, where each queue maintains its
state about the consumption of the messages.

## Motivation

OpenData-Queue is a message queue that allows producers to publish messages and
consumers to receive and process them.
In contrast to OpenData-Log, messages in OpenData-Queue are transient.
Once a message is consumed, the message is gone.
In other words, the retention of the messages is different compared to a log.
In a log the retention is time-based whereas in a queue the retention is consumption-based.
Another aspect in which a queue and a log differ is the order in which messages are consumed.
A log supports maintaining a complete history of what happened and allows to reprocess historical data.
Additionally, the order in which the messages are consumed might be different in a queue and a log.
A queue may re-order messages when messages could not be processed and need to be retried.
The typical use cases for a queue is spreading work across multiple workers.

OpenData-Queue allows to specify multiple queues.
When a message is published, the producer specifies the payload of the message and
data about the message, i.e., the metadata of the message.
A published message is assigned to a queue according to user-defined rules.
The same message can be assigned to multiple queues.
A consumer claims messages from a queue and acknowledges the successful processing
of the message or reports the unsuccessful processing of the message back to the queue.
Once a consumer acknowledges the successful processing of a message, that message
is added to the list of done messages of the queue.
If the consumer reports the unsuccessful processing of a message, that message is added to
the list of failed messages of the queue.

## Goals
- Specify a storage layout/data model for the queue

## Non-Goals
- Specification of a publish API for producers
- Specification of a consumption API for consumers
- Specification of an admin API to specify queues
- Specification of background processes that maintain the queues

## Design

OpenData-Queue stores the payload of the message and its metadata in two logically
separate stores, the payload store and the metadata log.
The payload store is optimized for point-lookups and the metadata log is optimized for
sequential scans.
The main benefit of this separation is that queues only need to scan the metadata
log to evaluate whether the message belongs to them without the need to also
read the potentially much larger payload.
Another benefit is that payloads of messages that belong to multiple queues do not
need to be copied into each queue.
Payloads are not read until they are claimed by consumers.

### Related message queue systems

#### Amazon SQS
Amazon Simple Queue Service (Amazon SQS) offers a secure, durable, and available hosted queue that lets you
integrate and decouple distributed software systems and components.

In SQS queues are identified by a queue URL (e.g., `https://sqs.us-east-1.amazonaws.com/123456789/my-queue`).

Each message is published to one specific queue.
A message consists of the queue URL and the message body.
Optionally also messages attributes (metadata) can be sent along with the message.
Messages can be published individually or in batches (up to 10).

A consumed message consists of the message body, the message ID, a receipt handle, an attributes (metadata).
Messages can be consumed in batches (up to 10).
A consumer can provide a visibility timeout which specifies how long a message is invisible to other consumers after consumption.
A consumed message must be explicitly deleted after processing otherwise they remain visible for other consumers.

SQS offers two queue types:
- standard (unlimited throughput, at-least-once delivery)
- FIFO (exactly-once, strict ordering)

Other features:
- Dead Letter Queues: Failed messages moved aside for inspection
- Retention: Up to 14 days
- Long polling: Wait up to 20 seconds for messages, reducing costs
- Batching: Send/receive/delete up to 10 messages per API call
- Server-side encryption: Via AWS KMS
- Auto-scaling: No capacity planning needed

#### RabbitMQ
RabbitMQ is an open-source message broker that enables applications to communicate asynchronously by sending and
receiving messages through queues.
Messages are not published directly to a queue; instead, the producer sends messages to an exchange.
An exchange is responsible for routing messages to different queues with the help of header attributes, bindings,
and routing keys.
Think of it like a post office or mail sorting facility.
RabbitMQ doesn't allow you to send a message to a queue directly, only through an exchange.
A binding is a "link" that you set up to bind a queue to an exchange,
and the routing key is a message attribute the exchange looks at when deciding how to route the message.
There are four types of exchange:
- Direct Exchange: A message goes to the queues whose binding key exactly matches the routing key of the message.
- Fanout Exchange: Routes a copy of every message published to them to every queue, stream or exchange bound to it.
  The message's routing key is completely ignored.
- Topic Exchange: Uses pattern matching of the message's routing key to the routing (binding) key pattern used at binding time.
- Headers Exchange: Designed for routing on multiple attributes that are more easily expressed as message headers than
  a routing key.

To send a message, you establish a connection to the RabbitMQ server,
create a channel, and send message with parameters for the exchange, routing key, and message body.
A message can contain various types of information, from process/task instructions to simple text.
RabbitMQ accepts, stores, and forwards binary blobs of data as messages.
Queues are specified by name.
To receive messages, consumers subscribe to a queue using a callback function that processes each message as it arrives.
The main features of RabbitMQ include support for multiple protocols (AMQP 1.0, MQTT 5.0),
flexible routing through different exchange types (direct, fanout, topic, headers),
and reliability through message acknowledgments and cluster replication.
An acknowledgment is sent back by the consumer to tell RabbitMQ that a message was received and processed,
and if a consumer dies without sending an ack, RabbitMQ will re-queue and redeliver the message to another consumer.
RabbitMQ is commonly used for decoupling microservices, implementing RPC patterns, streaming data,
and IoT applications—it can run on cloud environments, on-premises, or locally, and offers browser-based UI for
monitoring and management.

#### ActiveMQ
Apache ActiveMQ is an open-source message broker that implements the Java Message Service (JMS)API,
enabling asynchronous communication between distributed applications.
To send a message, you create a connection to the broker, establish a session, then use a MessageProducer to send
messages to a destination.
Messages can contain various content types including text (TextMessage),
serialized objects (ObjectMessage), key-value pairs (MapMessage), byte streams (BytesMessage),
or primitive streams (StreamMessage).
Receiving works similarly—you create a MessageConsumer on a destination and either call receive() to block until a
message arrives or register a MessageListener for asynchronous callback-based delivery.
The received message object contains the payload plus metadata like message ID, timestamp, correlation ID,
and custom properties.
Queues (for point-to-point messaging) and topics (for publish-subscribe) are specified by name when creating a
destination through the session, such as session.createQueue("orders.incoming").
ActiveMQ's main features include support for multiple protocols (OpenWire, STOMP, AMQP, MQTT, WebSocket),
message persistence for durability, transactions, clustering and high availability through networks of brokers,
wildcard destinations for flexible routing, and integration with Spring Framework.
The newer ActiveMQ Artemis variant offers improved performance with a non-blocking architecture and is recommended
for new projects.

### Related message queue protocols

**AMQP (Advanced Message Queuing Protocol):**
An open standard with rich routing capabilities including exchanges, bindings, and queues.
Supports transactions, acknowledgments, and multiple messaging patterns.
AMQP has strong adoption in enterprise and backend systems.
RabbitMQ alone has hundreds of thousands of production deployments.
It's the go-to for traditional message queuing, task distribution, and microservices communication where you need
rich routing and reliability features.
However, the total number of AMQP endpoints is far smaller than MQTT's device count.

**MQTT (Message Queuing Telemetry Transport):**
Lightweight publish/subscribe protocol designed for constrained devices and low-bandwidth networks.
Uses topic-based routing with three QoS levels.
MQTT is popular in IoT and is the most widely deployed messaging protocol.
It is very popular due to the sheer scale of IoT.
Billions of devices use it -- everything from smart home sensors to industrial equipment to connected vehicles.
Major cloud IoT platforms (AWS IoT Core, Azure IoT Hub, Google Cloud IoT) all support MQTT as their primary protocol.

**STOMP (Simple Text Oriented Messaging Protocol):**
A simple, text-based protocol that's easy to implement.
Works over WebSockets, making it useful for browser-based messaging.
STOMP is useful but niche.
It's typically chosen for browser-based messaging (via WebSockets) or
when simplicity matters more than performance.
No major messaging system uses STOMP as its primary protocol --
it's always an alternative alongside something else.
You'll find it supported in RabbitMQ and ActiveMQ,
but most users of those systems choose AMQP or OpenWire instead.

**OpenWire:**
OpenWire is ActiveMQ Classic's native binary protocol, optimized for performance with Java clients.
It was significant during ActiveMQ's peak popularity in the late 2000s and early 2010s when ActiveMQ was
one of the dominant open-source message brokers.

**Java Messaging Service:**
JMS is a messaging standard that allows Java EE application components to create, send, receive, and read messages,
enabling distributed communication that is loosely coupled, reliable, and asynchronous.
It is an API specification, not an implementation — vendors provide JMS providers (like ActiveMQ, IBM MQ, RabbitMQ).
The used wire protocol depends on the provider.


### Specification of records

All records in the OpenData-Queue system conform to the common record key prefix defined in [RFC 0001: Record Key Prefix](rfcs/0001-record-key-prefix.md).

```ascii
┌─────────┬────────────┬─────────────────────┐
│ version │    type    │     content         │
│ 1 byte  │   1 byte   │     (varies)        │
└─────────┴────────────┴─────────────────────┘
```

The initial version is 1.
The type specifies the type of the record.

Overview of record types in OpenData-Queue:

| type discriminator | Description     |
|--------------------|-----------------|
| `0x01`             | message payload |
| `0x02`             | active messages |
| `0x03`             | done messages   |
| `0x04`             | failed messages |
| `0x05`             | attempts        |
| `0x06`             | queue config    |

For the remaining of this RFC, we use `[version, type=<value>]` in the 
record encodings to denote the common record key prefix. 

### Payload store

The payload store stores the payload of the messages that are published to the message
queue system.
The payload store assigns an ID to each published messages.
The assigned ID is provided by a counter.
For the counter, the `SeqBlock` is used as described in [RFC 0002: Block-Based Sequence Allocation](/rfcs/0002-seq-block.md).

#### Payload record 

The payload record has the following encodings
```
Payload record:
Key:
┌──────────────────────────┬────────────┐
│ [version, type=0x01]     │ message ID │
│        (2 bytes)         │   (u64)    │
└──────────────────────────┴────────────┘

Value:
┌──────────────────────────────────────┐
│ record value (variable-length bytes) │
└──────────────────────────────────────┘
```

The key contains the assigned message ID.
The variable-length record value contains the payload as a sequence of bytes.
How to decode the payload is the responsibility of the consumer.
The length of the payload can be computed from the length of the value.

### Metadata log

The metadata log stores the metadata of the published messages.
The metadata log is an instance of [OpenData-Log](log/rfcs/0001-storage.md).

#### Metadata record

The key of the metadata record is `opendata-queue/{instance ID}/messages/metadata/{log ID}`.
The `instance ID` is the identifier of the OpenData-Queue system instance.
That is needed to distinguish between multiple OpenData-Queue systems that are maintained in the same SlateDB instance.
The `log ID` is the identifier of the metadata log.
An OpenData-Queue can maintain multiple metadata logs.
How the metadata of the published messages is distributed over the metadata logs depends on the implemented
distribution strategy.
One strategy could be to provide a metadata log for each created queue.
Another strategy could be to group messages according to the queue definitions.
While this RFC enables multiple metadata logs it does not specify any distribution strategy 
and leaves the specification for future RFCs.
The generation of both, `instance ID` and `log ID`, is still to be determined since they depend on how 
OpenData-Queue system instances will be deployed and on the distribution strategy of the metadata over the metadata logs.
The important aspect is that the key allows multiple OpenData-queues on the same SlateDB instance and that
an OpenData-queue allows to use multiple metadata logs.

The value of the metadata record is an encoded array of message metadata:
```
Metadata Record Value:
┌────────────────────────┐
│ Array<MessageMetadata> │
└────────────────────────┘
```

The size of the metadata array depends on the batching strategy.
Message metadata can be batched by time or by size or both.
The details of the batching message metadata is left for future RFCs.

The value contains the following metadata:
- the message ID assigned to the payload by the payload store
- the metadata encoded as an opaque byte field

The message metadata is encoded as follows:
```
MessageMetadata:
┌────────────┬───────────────────┐
│ message ID │     metadata      │
│   (u64)    │    (VarBytes)     │
└────────────┴───────────────────┘
```

with `VarBytes` encoded as:

```
┌────────────┬───────────────────┐
│   length   │       bytes       │
│   (u32)    │ (variable-length) │
└────────────┴───────────────────┘
```

The metadata is encoded as bytes prefixed with the length of the metadata.
The length of the metadata is required because `MessageMetadata` is an element of an array,
i.e., the start and the end of each element must be known for decoding the array. 
The queue is responsible for decoding the metadata it reads from the metadata log.

### Queue store

The queue store maintains the state and the config for each queue.

The state consists of active, done, and failed messages.

#### Active Messages Record

The record for the active messages contains which messages are available or in-flight. 
To limit the size of the record, the active messages are split into multiple buckets.
The size of each record can be specified by size.

The key of the active messages consists of:
- the common record key prefix with type discriminator `0x02`
- the identifier of the queue
- the identifier of the bucket

The key is encoded as follows:
```
Active Message Key:
┌──────────────────────┬───────────────────┬───────────┐
│ [version, type=0x02] │     queue ID      │ bucket ID │
│      (2 bytes)       │ (TerminatedBytes) │   (u64)   │
└──────────────────────┴───────────────────┴───────────┘
```
The queue ID is encoded as a `TerminatedBytes` to ensure lexicographical ordering. 
See [RFC-0004 Common Encodings](/rfcs/0004-common-encodings.md) for details. 
   
The bucket ID consists of two parts.
The highest 4 bits specify the capacity of the bucket in number of messages it can hold.
The capacity is computed as `10^(highest 4 bits)`.
If the highest 4 bits are `0000b = 0`, the capacity of the bucket is 1 message.
If the highest 4 bits are `0001b = 1`, the capacity of the bucket is 10 message, and so forth.
The maximal capacity of a bucket is `10^16`.
The remaining 60 bits are the number of the bucket.
The numbers of buckets start at 0 and increase by the capacity of the bucket.
Let's assume the capacity is 10. 
Thus, the first bucket has number 0, the second bucket has number 10, the third bucket number 20, etc. 

The value of the active messages consists of:
- the list of available messages, i.e., the messages that can be claimed by consumers
- the list of in-flight messages, i.e., the messages that are currently claimed by consumers

The encoding of the active messages value is as follows:
```
Active Message Value:
┌───────────────────────────┬──────────────────────────┐
│    available messages     │    in-flight messages    │
│ (Array<AvailableMessage>) │ (Array<InFlightMessage>) │
└───────────────────────────┴──────────────────────────┘
```

An `AvailableMessage` consists of:
- the message ID of the available message
- the claim ID of the message, i.e., the ID given to the consumer that claims the message
- the retention deadline of the message
- the number of attempts to claim the message
- the metadata of the message

The available message type is encoded as follows:
```
AvailableMessage:
┌────────────┬──────────┬──────────┬────────────┐
│ message ID │ claim ID │ attempts │  metadata  │
│   (u64)    │  (u64)   │  (u16)   │ (VarBytes) │
└────────────┴──────────┴──────────┴────────────┘
```
The claim ID consists of two parts similar to the bucket ID.
The highest 4 bits specify the capacity of a bucket at the time the claim ID was created.
The remaining 60 bits represent a number that uniquely identifies the message within a bucket of a given capacity.
The number of the bucket that contains the message with a given claim ID can be computed by integer dividing the 
number in the lower 60 bit with the capacity in the higher 4 bits and multiplying the result with the capacity.
That is `(number / capacity) * capacity`.
The claim ID is set when the message is read from the metadata log and assigned to the queue according
to the bucket it lands in.    

The metadata is again an opaque field.
The queue is responsible to decode the metadata.

An `InFlightMessage` consists of:
- the message ID of the in-flight message
- the claim ID of the message, i.e., the ID given to the consumer that claims the message
- the timestamp when the message was claimed
- the timestamp when the lease that was claimed ends at the latest
- the number of attempts to claim the message
- the metadata of the message

```
InFlightMessage:
┌────────────┬──────────┬────────────┬────────────────┬──────────┬────────────┐
│ message ID │ claim ID │ claimed at │ lease deadline │ attempts │  metadata  │
│   (u64)    │  (u64)   │   (i64)    │     (i64)      │  (u16)   │ (VarBytes) │
└────────────┴──────────┴────────────┴────────────────┴──────────┴────────────┘
```

There are multiple options to set the lease deadline.
Either the consumer that claims the message sets the deadline or the deadline is set
by the queue.

#### Done Messages Record

Besides the available and in-flight messages, the queue store also maintains the list of done messages per queue.
A message is done if a consumer claimed the message and acknowledged the successful processing.
Also, here, to limit the size of the record, the done messages are split into multiple buckets.
The done message have a retention period after which the done messages are removed from the queue.
The payload and the metadata of the messages are not removed from the system since other queues could still use them.
The key for the done messages is encoded as follows:
```
Done Messages key:
┌──────────────────────┬───────────────────┬───────────┐
│ [version, type=0x03] │      queue ID     │ bucket ID │
│        (2 bytes)     │ (TerminatedBytes) │   (u64)   │
└──────────────────────┴───────────────────┴───────────┘
```
The key is similar to the key for the available messages, but with a different type discriminator, i.e., `0x03`.

The done messages value is an array of done messages.
```
Done Messages value:
┌──────────────────────┐
│     done messages    │
│ (Array<DoneMessage>) │
└──────────────────────┘
```
The `DoneMessage` type consists of:
- the message ID
- the claim ID of the message
- the timestamp when the consumer acknowledged successful processing
- the metadata of the message

```
DoneMessage:
┌────────────┬──────────┬─────────┬──────────┬────────────┐
│ message ID │ claim ID │ done at │ attempts │  metadata  │
│   (u64)    │  (u64)   │  (i64)  │  (u16)   │ (VarBytes) │
└────────────┴──────────┴─────────┴──────────┴────────────┘
```

#### Failed Messages Record

Finally, a list of unsuccessfully processed messages keeps the messages that could not be processed after
the configured number of attempts.
Similar to active and done messages, also failed messages are split into multiple buckets.
The key for the failed messages is encoded as follows:
```
Failed Messages Key:
┌──────────────────────┬───────────────────┬───────────┐
│ [version, type=0x04] │      queue ID     │ bucket ID │
│      (2 bytes)       │ (TerminatedBytes) │   (u64)   │
└──────────────────────┴───────────────────┴───────────┘
```
The key is similar to the key for the available messages, but with a different type discriminator, i.e., `0x04`.

The failed messages value is an array of failed messages.
```
Failed Messages value:
┌────────────────────────┐
│    failed messages     │
│ (Array<FailedMessage>) │
└────────────────────────┘
```
The `FailedMessage` type consists of:
- the message ID
- the claim ID
- the timestamp when the message was considered failed
- the attempts to process the message
- the metadata of the message

```
FailedMessage:
┌────────────┬──────────┬───────────┬──────────┬────────────┐
│ message ID │ claim ID │ failed at │ attempts │  metadata  │
│   (u64)    │  (u64)   │   (i64)   │  (u16)   │ (VarBytes) │
└────────────┴──────────┴───────────┴──────────┴────────────┘
```
A message is considered failed if a consumer reports unsuccessful processing and there are no more attempts left for
the message or if the lease deadline expired and there are no more attempts left for the message.
The maximum attempts are configured per queue.

#### Attempts Record

Each attempt to claim the message is recorded in the attempts record.
The key of the attempts record is encoded as follows:

```
Attempts Key:
┌──────────────────────┬───────────────────┬────────────┐
│ [version, type=0x05] │      queue ID     │ message ID │
│      (2 bytes)       │ (TerminatedBytes) │    (u64)   │
└──────────────────────┴───────────────────┴────────────┘
```
The key of the attempts has the common key prefix with type discriminator (`0x05`), the queue ID, and the message ID.

The value of the attempts are encoded as follows:
```
Attempts Value:
┌──────────────────┐
│     attempts     │
│ (Array<Attempt>) │
└──────────────────┘
```
The value of the attempts are encoded as an array of `Attempt`s.

An `Attempt` is encoded as follows:
```
Attempt:
┌────────────┬──────────────┐
│ claimed at │  consumer ID │
│   (i64)    │ (ConsumerID) │
└────────────┴──────────────┘
```
To add new attempts the merge operator can be used since attempts are only added and never removed.

##### Queue Specification record

The queue store also contains the specification of the queue.

The key of the configuration contains the common key prefix with type discriminator `0x06`.
```
Queue Spec Key:
┌──────────────────────┬──────────┐
│ [version, type=0x06] │ queue ID │
│      (2 bytes)       │  (bytes) │
└──────────────────────┴──────────┘
```

The specification consists of:
- the filters that specify which messages the queue contains.
- the maximum attempts to process a message
- the retention duration for done messages
- the bucket size for the various message records
- the description of the queue 

```
Queue Spec Value:
┌─────────────────────────────────────────────────┐
│                    filters                      │
│               (FilterExpression)                │
├─────────────────────────────────────────────────┤
│                  max attempts                   │
│                     (u16)                       │
├─────────────────────────────────────────────────┤
│ retention duration for done messages in seconds │
│                     (u32)                       │
├─────────────────────────────────────────────────┤
│                  bucket size                    │
│                     (u8)                        │
├─────────────────────────────────────────────────┤
│                   description                   │
│                     (Utf8)                      │
└─────────────────────────────────────────────────┘
```

Added messages that satisfy the filter expression for a specific queue belong to the queue and can be claimed by
consumers that read the queue.
The filter expression specifies conditions over the message metadata.
The exact grammar and encoding of the filter expressions are still to be determined.

After the maximum attempts the message is moved to the failed messages.

The retention duration for done messages is computed from the timestamp `done at` in the `DoneMessage` record value.
If the current timestamp exceeds the `done at` timestamp plus the retention duration, the done message is
removed from the array of done messages in the `DoneMessages` value.
A retention duration of `0` means infinite retention.

Finally, the queue also contains a description.

### Workflow overview

The OpenData-Queue has the following rough workflow
1. an admin specifies queues with filter expressions
   1. the queue configuration is stored in the queue store
   2. an empty active messages record, an empty done messages record, and an empty failed messages record for the queue are added
2. a producer adds messages
   1. the payload of the message is stored in the payload store
   2. the metadata of the message is stored in the metadata log
3. a process tails the metadata log and adds the messages to the available messages
4. a consumer consumes messages in a queue
   1. the consumer claims an available message
   2. the message is removed from the available messages and added to the in-flight messages
   3. the consumer acknowledges the successful processing of a message
      1. the message is removed from the in-flight messages and added to the done messages
   4. the consumer reports the unsuccessful processing of a message
      1. if the message has still attempts, the message is removed from the in-flight messages and added to the available messages
      2. if the message has no attempts left, the message is removed from the in-flight messages and added to the failed messages

## Alternatives

### Use a record for each available and in-flight message
With such an approach, each available message would have a key like `/avail/{queue ID}/{message ID}` and
each in-flight queue a key like `/inflight/{queue ID}/{message ID}`.
Each transition from available to in-flight would be a prefix scan of `/avail/{queue ID}` to get all available
messages, then a put of `/inflight/{queue ID}/{message ID}` and then a delete of `/avail/{queue ID}/{message ID}`.
With the proposed approach, the transition from available to in-flight consists of a point lookup `/state/{queue ID}` and
a put of `/state/{queue ID}` which should be more efficient.

### Use one record for all available messages and all in-flight messages
In this case, each transition from available to in-flight would be a point lookup of `/avail/{queue ID}` to get all available
messages, a point lookup of `/inflight/{queue ID}`, then a put of `/avail/{queue ID}` as well as `/inflight/{queue ID}`.
The proposed approach saves one point lookup and one put.

### Store done messages together with available and in-flight messages
The array of done messages might grow indefinitely if it has infinite retention.
However, available and in-flight messages need to be queried and written continuously to claim messages.
Each point lookup would load the entire list of done messages.
Each write would write an updated record containing the entire list of done messages, which would result in
a lot of duplicate data that is stored and retrieved until the compactor gets rid of it.
Since most of the time messages are only added to the list of done messages, a merge operator can be specified.
The deletion of done message due to the retention period can be done in the background once in a while.


## Open Questions
- Grammar and encoding of filter expressions:
  - What operators and functions should be allowed?
  - Should the priority of a message depend on the filter expression?
  - How should the expression be encoded in the record?
- The type of the consumer ID is still to be determined:
  - Should the queue give an ID to a consumer or should the consumer determine its ID?
- Should the queue set the lease deadline or should the consumer set it, or both?
- Should we specify the queue spec (except the filter expression) as a key-value list?
- How can a strategy for distributing message metadata over metadata logs be persisted? 

## Updates

| Date       | Description                         |
|------------|-------------------------------------|
| 2026-01-26 | Initial draft                       |
| 2026-02-4  | Major iteration, introduced buckets |
