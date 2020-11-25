using KVStoreServer.CausalConsistency;
using KVStoreServer.KVS;
using KVStoreServer.Naming;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KVStoreServer.Broadcast {
    class PartitionReliableBroadcastHandler {

        private string selfId;
        private string partitionId;
        private ImmutableHashSet<string> serverIds;

        private int messageCounter = 0;

        private ConcurrentBag<MessageId> receivedMessages = new ConcurrentBag<MessageId>();
        private ConcurrentDictionary<MessageId, int> messageAcks = new ConcurrentDictionary<MessageId, int>();

        public PartitionReliableBroadcastHandler(
            string selfId,
            string partitionId,
            ImmutableHashSet<string> serverIds) {

            this.selfId = selfId;
            this.partitionId = partitionId;
            this.serverIds = serverIds;
        }

        //public void BroadcastWrite(
        //    string key,
        //    ImmutableTimestampedValue value,
        //    ImmutableVectorClock replicaTimestamp,
        //    long timeout) {
            
        //    MessageId messageId = NextMessageId();
        //    foreach(string serverId in serverIds) {
        //        SimpleNamingServiceLayer.Instance.BroadcastWrite(
        //            serverId,
        //            messageId,
        //            key,
        //            value,
        //            replicaTimestamp,
        //            timeout)
        //        .ContinueWith(antecedent => {
        //            messageAcks.AddOrUpdate(messageId, 1, (key, prev) => prev + 1);
        //        });    
        //    }
        //}

        private MessageId NextMessageId() {
            lock(this) {
                MessageId messageId = new MessageId(selfId, messageCounter);
                messageCounter++;
                return messageId;
            }
        }
    }
}
