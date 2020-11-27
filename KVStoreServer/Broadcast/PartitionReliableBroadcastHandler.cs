using KVStoreServer.CausalConsistency;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.KVS;
using KVStoreServer.Naming;
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

        private readonly ConcurrentDictionary<MessageId, BroadcastWriteMessage> pendingWrites = 
            new ConcurrentDictionary<MessageId, BroadcastWriteMessage>();

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
            BroadcastWrite(messageId, key, value, replicaTimestamp);
            
            lock(pendingWrites) {
                // Wait for one ack
                // First time message is reinserted is the first ack
                while(!pendingWrites.ContainsKey(messageId)) {
                    Monitor.Wait(pendingWrites);
                }
                pendingWrites.TryGetValue(messageId, out BroadcastWriteMessage message);
                writeMessageHandler(message);
            }
        }

        public void OnBroadcastWriteDeliver(BroadcastWriteArguments arguments) {
            lock(pendingWrites) {
                if (!pendingWrites.ContainsKey(arguments.MessageId)
                    && pendingWrites.TryAdd(
                        arguments.MessageId, 
                        BuildWriteMessage(arguments))) {

                    BroadcastWrite(
                        arguments.MessageId,
                        arguments.Key,
                        arguments.TimestampedValue,
                        arguments.ReplicaTimestamp);

                    Monitor.PulseAll(pendingWrites);
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
