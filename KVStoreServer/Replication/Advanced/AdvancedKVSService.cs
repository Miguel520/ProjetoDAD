using KVStoreServer.Broadcast;
using KVStoreServer.CausalConsistency;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Storage.Advanced;
using System.Collections.Generic;
using System.Threading;

namespace KVStoreServer.Replication.Advanced {
    public class AdvancedKVSService {

        private readonly ServerConfiguration serverConfig;

        private readonly MutableVectorClock timestamp = MutableVectorClock.Empty();

        private readonly AdvancedPartitionedKVS store = new AdvancedPartitionedKVS();

        public AdvancedKVSService(ServerConfiguration serverConfig) {
            this.serverConfig = serverConfig;
        }

        public void Bind() {
            ReliableBroadcastLayer.Instance.BindWriteHandler(OnWriteRequest);
            ReliableBroadcastLayer.Instance.BindListServerHandler(OnListServerRequest);
            ReliableBroadcastLayer.Instance.BindWriteMessageHandler(OnBroadcastWriteMessage);
        }

        /*
         * Handle write request from client
         */
        public ImmutableVectorClock OnWriteRequest(WriteArguments arguments) {
            ImmutableTimestampedValue valueToBroadcast;
            ImmutableVectorClock timestampToBroadcast;

            WaitHappensBeforeTimestamp(arguments.Timestamp);
            
            lock(this) {
                valueToBroadcast = store.PrepareWrite(
                    arguments.PartitionId,
                    arguments.ObjectId,
                    arguments.ObjectValue,
                    serverConfig.ServerId);
                timestamp.Increment(serverConfig.ServerId);
                timestampToBroadcast = timestamp.ToImmutable();
            }

            // This waits for deliver and executes write
            ReliableBroadcastLayer.Instance.BroadcastWrite(
                arguments.PartitionId,
                arguments.ObjectId,
                valueToBroadcast,
                timestampToBroadcast);

            return timestampToBroadcast;
        }

        /*
         * Handle list server request from client
         */
        public IEnumerable<StoredObjectDto> OnListServerRequest() {
            return store.ListObjects();
        }

        /*
         * Handle write message from the network
         */
        public void OnBroadcastWriteMessage(BroadcastWriteMessage message) {
            lock(this) {
                store.Write(message.PartitionId, message.Key, message.TimestampedValue);
                MergeTimestamp(message.ReplicaTimestamp);
            }
        }

        private void WaitHappensBeforeTimestamp(ImmutableVectorClock otherTimestamp) {
            lock(timestamp) {
                while (VectorClock.HappensBefore(timestamp, otherTimestamp)) {
                    Monitor.Wait(timestamp);
                }
            }
        }

        private void MergeTimestamp(ImmutableVectorClock otherTimestamp) {
            lock(timestamp) {
                timestamp.Merge(otherTimestamp);
                Monitor.PulseAll(timestamp);
            }
        }
    }
}
