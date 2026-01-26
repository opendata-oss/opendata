# RFC 0001: Queue Storage

**Status**: Draft

**Authors**:
- [Bruno Cadonna](https://github.com/cadonna>) 

## Summary

This RFC defines the storage format for OpenData-Queue. 
The design separates message storage from queue state, where each queue maintains its 
own append-only sequence of records tracking message consumption state.

## Motivation

OpenData-Queue is a message queue that allows producers to publish messages and 
consumers to receive and process them.
OpenData-Queue allows to specify multiple queues.
When a messages is published, the producer specifies the payload of the message and 
data about the message, i.e., the metadata of the message.
For example, the metadata contains when the message is visible for consumption.
A published message is assigned to a queue according to user-defined rules.
User defined rules are formulated for each queue by filtering on the metadata of the messages.
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
- Specification of a admin API to specify queues
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

### Payload store

The payload store stores the payload of the messages that are published to the message 
queue system. 
The payload store assigns an ID to each published messages.
The assigned ID is provided by a counter.
For the counter, the same technique as for the sequence number in [OpenData-Log](log/rfcs/0001-storage.md) shall be used.

The key of a payload in the payload store conforms to 
[RFC 0001: Record Key Prefix](rfcs/0001-record-key-prefix.md). 

```
Payload Entry:
    SlateDB Key:   | version (u8) | type (u8) | message ID (u64)
    SlateDB Value: | record value (bytes)
```

The initial version is 1. 
The type discriminator `0x01` is reserved for payload entries. 
Additional record types (e.g., indexes) may be introduced in future RFCs 
using different discriminators.

The record value has the following encoding
```
Payload Record Value:
    | type of payload (u8) | payload (variable-length bytes)
```
The type of payload specifies the format of the payload.
Initially, there will be two payload types:
- `0x01` `Array<Byte>`  
- `0x02` URI (encoding still to be determined)

An `Array<T>` is encoded as a `count (u16, little endian)` followed by `count` serialized elements of type T,
encoded back-to-back with no additional padding. 
The type is defined in [OpenData-Timeseries](timeseries/rfcs/0001-tsdb-storage.md).

### Metadata log

The metadata log stores the metadata of the published messages.
The metadata log is an [OpenData-Log](log/rfcs/0001-storage.md).
The key of the log is `opendata-queue/messages/metadata`.
If multiple queue systems are maintained by the same slateDB instance, the key for the 
metadata log needs to be suffixed by an identifier of the actual OpenData-Queue 
instance, e.g., `/newsfeeds`.

The record value contains the following metadata:
- the message ID assigned to the payload by the payload store
- the timestamp the message was created
- the timestamp the message was received
- the timestamp after which the message becomes visible the first time to consumers
- the headers of the message (key-value pairs) that contain data for filtering and routing

The record value is encoded as follows:
```
Metadata Record Value:
    | message ID (u64)
    | created at (i64)
    | received at (i64)
    | visible at (i64)
    | retention duration in seconds (u32)
    | number of headers N (u8)
    | key 0 (Utf8) | value 0 (Utf8)   
    ...
    | key N-1 (Utf8) | value N-1 (Utf8)   
```
The type `Utf8` is specified in [OpenData-Timeseries](timeseries/rfcs/0001-tsdb-storage.md)
The retention duration in seconds is computed from the `received at` timestamp.
If the current timestamp exceeds the `received at` timestamp plus the retention duration, 
the message -- payload and metadata -- are removed from the OpenData-queue system.
A retention duration of `0` means infinite retention.

### Queue store

The queue store maintains the state and the config for each queue.

The state consists of active, done, and failed messages.

The key of the active messages consists of:
- the version of the key
- the type of key, i.e., key of the active messages
- The identifier of the queue

The key is encoded as follows:
```
Active Message Key:
    | version (u8) | type (u8) | queue ID (TerminatedBytes)   
```
The initial version is 1.
The type discriminator is `0x02`. 

The queue ID is of type `TerminatedBytes`.
A `TerminatedBytes` is a variable-length byte sequence that terminates with a 0x00 delimiter as described in 
[OpenData-Log](log/rfcs/0001-storage.md).

The value of the active messages consists of:
- the list of available messages, i.e., the messages that can be claimed by consumers
- the list of in-flight messages, i.e., the messages that are currently claimed by consumers 

The encoding of the active messages value is as follows:
```
Queue State Value:
    | available messages (Array<AvailableMessage>)
    | in-flight messages (Array<InFlightMessage>)   
```

An `AvailableMessage` consists of:
- the message ID of the available message
- the priority of the available message
- the timestamp after which the message becomes visible to consumers
- the retention deadline of the message.
- the number attempts to claim the message.

The available message type is encoded as follows:
```
AvailableMessage:
    | message ID (u64) | priority (u8) | visible at (i64) | retention deadline (i64) | attempts (u16)   
```
The priority describes the priority of the message in this queue.
The priority decreases with increasing number.
For example, message with priority 1 has a higher priority than a message with priority 2.
Available messages with higher priorities need to be claimed before messages with lower priority.

An available message with a `visible at` timestamp after the current timestamp cannot be claimed
by a consumer.
The `visible at` timestamp is set by the message metadata.
However, it could also be used to implement a back-off mechanism when processing attempts fail.

The retention deadline is computed by adding the retention duration in the message metadata
to the received at timestamp.
If the retention deadline is expired, the message is removed from the payload store.

An `InFlightMessage` consists of:
- the message ID of the available message
- the priority of the available message
- the timestamp when the message was claimed
- the timestamp when the lease that was claimed ends at the latest
- the number of attempts to claim the message

```
InFlightMessage:
    | message ID (u64) | priority (u8) | | claimed at (i64) | lease deadline (i64) | attempts (u16)   
```

There are multiple options to set the lease deadline. 
Either the consumer that claims the message sets the deadline or the deadline is set 
by the queue config.  

Besides the available and in-flight messages, the queue store also maintains the list of done messages per queue.
A message is done if a consumer claimed the message and acknowledged the successful processing. 
The done message have a retention period after which the done messages are removed from the queue.
The payload and the metadata of the messages are not removed from the system since other queues could still use them.
The key for the done messages is encoded as follows:
```
Done Messages key:
    | version (u8) | type (u8) | queue ID (TerminatedBytes)
```
The key is similar to the key for the available messages, but with a different type discriminator, i.e., `0x03`.

The done messages value is an array of done messages.
```
Done Messages value:
    | done messages (Array<DoneMessage>)
```
The `DoneMessage` type consists of:
- the message ID
- the timestamp when the consumer acknowledged successful processing
- the number of attempts to claim the message

```
DoneMessage:
    | message ID (u64) | done at (i64) | attempts (u16)
```

Finally, a list of unsuccessfully processed messages keeps the messages that could not be processed after
the configured number of attempts.
The key for the failed messages is encoded as follows:
```
Failed Messages Key:
    | version (u8) | type (u8) | queue ID (TerminatedBytes)
```
The key is similar to the key for the available messages, but with a different type discriminator, i.e., `0x04`.

The failed messages value is an array of failed messages.
```
Failed Messages value:
    | failed messages (Array<FailedMessage>)
```
The `FailedMessage` type consists of:
- the message ID
- the timestamp when the message was considered failed
- the attempts to process the message

```
FailedMessage:
    | message ID (u64) | failed at (i64) | attempts (u16)
```
A message is considered failed if a consumer reports unsuccessful processing and there are no more attempts left for 
the message or if the lease deadline expired and there are no more attempts left for the message.
The maximum attempts are configured per queue.

The failed attempts are recorded in the attempts record.
The key of the attempts record is encoded as follows:

```
Attempts Key:
    | version (u8) | type (u8) | message ID (u64)
```
The key of the attempts has a version (initially 1), the type discriminator (`0x05`), and the message ID.

The value of the attempts are encoded as follows: 
```
Attempts Value:
    | attempts (Array<ConsumerID>)
```
The value of the attempts are encoded as an array of consumer IDs.
The array contains the IDs of the consumers that claimed the message in ascending time order of the claims.

Additionally to the active messages, the done messages, and the failed messages, the queue store also contains the configuration of the queue.

The key of the configuration is similar to the key of the available messages but with a different type discriminator, 
i.e., `0x06`.
```
Queue Config Key:
    | version (u8) | type (u8) | queue ID (TerminatedBytes)
```

The config consists of:
- the filters that specify which messages the queue contains.
- the maximum attempts to process a message
- the retention duration for done messages

```
Queue Config Value:
    | filters (FilterExpression)
    | max attempts (u16)
    | retention duration for done messages in seconds (u32)
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

### Record types overview

| type discriminator | Description     |
|--------------------|-----------------|
| `0x01`             | message payload |
| `0x02`             | active messages |
| `0x03`             | done messages   |
| `0x04`             | failed messages |
| `0x05`             | attempts        |
| `0x06`             | queue config    |

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
- How to encode the URI for the payload?
- Should the queue set the lease deadline or should the consumer set it, or both?
- Should we specify the queue config (except the filter expression) as a key-value list?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-26 | Initial draft |
