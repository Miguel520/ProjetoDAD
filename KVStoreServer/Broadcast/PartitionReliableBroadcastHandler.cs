using Common.CausalConsistency;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Naming;
using KVStoreServer.Storage.Advanced;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Threading;

namespace KVStoreServer.Broadcast {
    class PartitionReliableBroadcastHandler {

        private readonly string selfId;
        private readonly string partitionId;
        private ImmutableHashSet<string> serverIds;

        private readonly WriteMessageHandler writeMessageHandler;

        private int messageCounter = 0;

        private readonly ConcurrentDictionary<MessageId, BroadcastWriteMessage> receivedWrites = 
            new ConcurrentDictionary<MessageId, BroadcastWriteMessage>();
        private readonly ConcurrentDictionary<MessageId, int> writesAcks =
            new ConcurrentDictionary<MessageId, int>();

        private readonly ConcurrentDictionary<MessageId, BroadcastFailureMessage> receivedFailures =
            new ConcurrentDictionary<MessageId, BroadcastFailureMessage>();

        public PartitionReliableBroadcastHandler(
            string selfId,
            string partitionId,
            ImmutableHashSet<string> serverIds,
            WriteMessageHandler writeMessageHandler) {

            this.selfId = selfId;
            this.partitionId = partitionId;
            this.serverIds = serverIds;
            this.writeMessageHandler = writeMessageHandler;
        }

        public void BroadcastWrite(
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp) {

            MessageId messageId = NextMessageId();

            // Broadcast messages
            receivedWrites.TryAdd(messageId, new BroadcastWriteMessage {
                PartitionId = partitionId,
                Key = key,
                TimestampedValue = value,
                ReplicaTimestamp = replicaTimestamp
            });

            BroadcastWrite(messageId, key, value, replicaTimestamp);

            
            lock (this) {
                // Wait for 2 acks
                // First time message is reinserted is the first ack
                while(!writesAcks.TryGetValue(messageId, out int numAcks)
                    && numAcks < 2 && serverIds.Count >= 2) {
                    Monitor.Wait(this);
                }
                receivedWrites.TryGetValue(messageId, out BroadcastWriteMessage message);
                writeMessageHandler(message);
            }
        }

        public void OnBroadcastWriteDeliver(BroadcastWriteArguments arguments) {
            lock(this) {
                // Add ack
                MessageId messageId = arguments.MessageId;
                writesAcks.AddOrUpdate(messageId, 1, (key, prev) => prev + 1);

                // Broadcast if necessary
                if (!receivedWrites.ContainsKey(arguments.MessageId)) {

                    receivedWrites.TryAdd(messageId, BuildWriteMessage(arguments));

                    // Other server received the request from client
                    // Broadcast and wait
                    BroadcastWrite(
                        arguments.MessageId,
                        arguments.Key,
                        arguments.TimestampedValue,
                        arguments.ReplicaTimestamp);
                    receivedWrites.TryGetValue(arguments.MessageId,
                        out BroadcastWriteMessage message);
                }

                // Check if can deliver
                if (writesAcks.TryGetValue(messageId, out int numAcks)
                    && numAcks == 2) {

                    if (messageId.SenderId == selfId) {
                        Monitor.PulseAll(this);
                    }
                    else {
                        writeMessageHandler(BuildWriteMessage(arguments));
                    }
                }
            }
        }

        public void BroadcastFailure(string failedServerId) {

            if (!serverIds.Contains(failedServerId))
                return;

            MessageId messageId = NextMessageId();

            receivedFailures.TryAdd(messageId, new BroadcastFailureMessage {
                PartitionId = partitionId,
                FailedServerId = failedServerId
            });

            // Broadcast messages
            BroadcastFailure(messageId, failedServerId);
        }

        public void OnBroadcastFailureDeliver(BroadcastFailureArguments arguments) {
            lock (receivedFailures) {
         
                if (!receivedFailures.ContainsKey(arguments.MessageId)) {

                    receivedFailures.TryAdd(
                        arguments.MessageId,
                        BuildFailureMessage(arguments));

                    BroadcastFailure(
                        arguments.MessageId,
                        arguments.FailedServerId);
                }
            }
        }

        private void BroadcastWrite(
            MessageId messageId,
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp) {

            foreach (string serverId in serverIds) {
                _ = AdvancedNamingServiceLayer.Instance.BroadcastWrite(
                    serverId,
                    partitionId,
                    messageId,
                    key,
                    value,
                    replicaTimestamp);
            }
        }

        private void BroadcastFailure(
            MessageId messageId,
            string failedServerId) {

            serverIds = serverIds.Remove(failedServerId);

            lock(this){
                if (serverIds.Count < 2) {
                    Monitor.PulseAll(this);
                }
            }

            foreach (string serverId in serverIds) {
                _ = AdvancedNamingServiceLayer.Instance.BroadcastFailure(
                    serverId,
                    partitionId,
                    messageId,
                    failedServerId);
            }
        }


        private MessageId NextMessageId() {
            lock (this) {
                MessageId messageId = new MessageId(selfId, messageCounter);
                messageCounter++;
                return messageId;
            }
        }

        private BroadcastWriteMessage BuildWriteMessage(BroadcastWriteArguments arguments) {
            return new BroadcastWriteMessage {
                PartitionId = arguments.PartitionId,
                Key = arguments.Key,
                TimestampedValue = arguments.TimestampedValue,
                ReplicaTimestamp = arguments.ReplicaTimestamp
            };
        }

        private BroadcastFailureMessage BuildFailureMessage(BroadcastFailureArguments arguments) {
            return new BroadcastFailureMessage {
                PartitionId = arguments.PartitionId,
                FailedServerId = arguments.FailedServerId
            };
        }
    }
}
