using Common.CausalConsistency;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Naming;
using KVStoreServer.Storage.Advanced;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Threading;

namespace KVStoreServer.Broadcast {
    class PartitionReliableBroadcastHandler {

        private readonly string selfId;
        private readonly string partitionId;
        private readonly ImmutableHashSet<string> serverIds;

        private readonly WriteMessageHandler writeMessageHandler;
        private readonly FailureMessageHandler failureMessageHandler;

        private int messageCounter = 0;

        private readonly ConcurrentDictionary<MessageId, BroadcastWriteMessage> receivedWrites = 
            new ConcurrentDictionary<MessageId, BroadcastWriteMessage>();
        private readonly ConcurrentDictionary<MessageId, int> writesAcks =
            new ConcurrentDictionary<MessageId, int>();

        private readonly ConcurrentDictionary<MessageId, BroadcastFailureMessage> pendingFailures =
            new ConcurrentDictionary<MessageId, BroadcastFailureMessage>();

        public PartitionReliableBroadcastHandler(
            string selfId,
            string partitionId,
            ImmutableHashSet<string> serverIds,
            WriteMessageHandler writeMessageHandler,
            FailureMessageHandler failureMessageHandler) {

            this.selfId = selfId;
            this.partitionId = partitionId;
            this.serverIds = serverIds;
            this.writeMessageHandler = writeMessageHandler;
            this.failureMessageHandler = failureMessageHandler;
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
                    && numAcks == 2) {
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

            MessageId messageId = NextMessageId();

            // Broadcast messages
            BroadcastFailure(messageId, failedServerId);

            lock (pendingFailures) {
                // Wait for one ack
                // First time message is reinserted is the first ack
                while (!pendingFailures.ContainsKey(messageId)) {
                    Monitor.Wait(pendingFailures);
                }
                pendingFailures.TryGetValue(messageId, out BroadcastFailureMessage message);
                failureMessageHandler(message);
            }
        }

        public void OnBroadcastFailureDeliver(BroadcastFailureArguments arguments) {
            lock (pendingFailures) {
                if (!pendingFailures.ContainsKey(arguments.MessageId)
                    && pendingFailures.TryAdd(
                        arguments.MessageId,
                        BuildFailureMessage(arguments))) {

                    BroadcastFailure(
                        arguments.MessageId,
                        arguments.FailedServerId);

                    Monitor.PulseAll(pendingFailures);
                }
            }
        }

        private MessageId NextMessageId() {
            lock (this) {
                MessageId messageId = new MessageId(selfId, messageCounter);
                messageCounter++;
                return messageId;
            }
        }

        private void BroadcastWrite(
            MessageId messageId,
            string key,
            ImmutableTimestampedValue value,
            ImmutableVectorClock replicaTimestamp) {

            foreach (string serverId in serverIds) {
                System.Console.WriteLine("Sending message to {0}", serverId);
                _ = AdvancedNamingServiceLayer.Instance.BroadcastWrite(
                    serverId,
                    partitionId,
                    messageId,
                    key,
                    value,
                    replicaTimestamp);
            }
        }

        public void BroadcastFailure(
            MessageId messageId,
            string failedServerUrl) {

            foreach (string serverId in serverIds) {
                _ = AdvancedNamingServiceLayer.Instance.BroadcastFailure(
                    serverId,
                    partitionId,
                    messageId,
                    failedServerUrl);
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
