using Common.CausalConsistency;
using KVStoreServer.Broadcast;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Grpc.Base;
using KVStoreServer.Replication.Base;
using KVStoreServer.Storage.Advanced;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;

namespace KVStoreServer.Replication.Advanced {
    public class AdvancedReplicationService {

        private readonly AdvancedPartitionsDB partitionsDB;

        private readonly ServerConfiguration serverConfig;

        private readonly MutableVectorClock timestamp = MutableVectorClock.Empty();

        private readonly AdvancedPartitionedKVS store = new AdvancedPartitionedKVS();

        public AdvancedReplicationService(
            AdvancedPartitionsDB partitionsDB,
            ServerConfiguration serverConfig) {

            this.partitionsDB = partitionsDB;
            this.serverConfig = serverConfig;
            ReliableBroadcastLayer.Instance.RegisterSelfId(this.serverConfig.ServerId);
        }

        public void Bind() {
            ReliableBroadcastLayer.Instance.BindWriteHandler(OnWriteRequest);
            ReliableBroadcastLayer.Instance.BindListServerHandler(OnListServerRequest);
            ReliableBroadcastLayer.Instance.BindWriteMessageHandler(OnBroadcastWriteMessage);
            ReliableBroadcastLayer.Instance.BindStatusHandler(OnStatus);

            ReliableBroadcastLayer.Instance.BindJoinPartitionHandler(OnJoinPartitionRequest);
            ReliableBroadcastLayer.Instance.BindLookupMasterHandler(TryGetMasterUrl);
            ReliableBroadcastLayer.Instance.BindListPartitionsHandler(ListPartitionsWithServerIds);
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
                Console.WriteLine("Received broadcast of value {0}", message.TimestampedValue.Value);
                store.Write(message.PartitionId, message.Key, message.TimestampedValue);
                MergeTimestamp(message.ReplicaTimestamp);
            }
        }

        public void OnJoinPartitionRequest(JoinPartitionArguments arguments) {
            string partitionId = arguments.PartitionId;
            partitionsDB.JoinPartition(arguments);
            partitionsDB.TryGetPartition(partitionId, out ImmutableHashSet<string> serverIds);
            
            // Only register partitions that the server belongs to for broadcast
            if (serverIds.Contains(serverConfig.ServerId)) {
                ReliableBroadcastLayer.Instance.RegisterPartition(partitionId, serverIds);
            }
        }

        public bool TryGetMasterUrl(string partitionId, out string masterUrl) {
            masterUrl = default;
            return partitionsDB.TryGetMasterUrl(partitionId, out string masterId)
                && ReliableBroadcastLayer.Instance.TryGetServer(masterId, out masterUrl);
        }

        public ImmutableList<PartitionServersDto> ListPartitionsWithServerIds() {
            return partitionsDB.ListPartitionsWithServerIds();
        }

        public void OnStatus() {
            Console.WriteLine(
                "[{0}] Server with id {1} running at {2}",
                 DateTime.Now.ToString("HH:mm:ss"),
                 serverConfig.ServerId,
                 serverConfig.Url);

            Console.WriteLine(
                "[{0}]  Partitions: {1}",
                DateTime.Now.ToString("HH:mm:ss"),
                string.Join(", ", partitionsDB.ListPartitions()));
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
