using Common.CausalConsistency;
using Common.Utils;
using KVStoreServer.Broadcast;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Advanced;
using KVStoreServer.Grpc.Base;
using KVStoreServer.Replication.Base;
using KVStoreServer.Storage.Advanced;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;

namespace KVStoreServer.Replication.Advanced {
    public class AdvancedReplicationService {

        private readonly AdvancedPartitionsDB partitionsDB;

        private readonly ServerConfiguration serverConfig;

        private readonly ConcurrentDictionary<string, MutableVectorClock> timestamps =
            new ConcurrentDictionary<string, MutableVectorClock>();

        private readonly AdvancedPartitionedKVS store = new AdvancedPartitionedKVS();

        public AdvancedReplicationService(
            AdvancedPartitionsDB partitionsDB,
            ServerConfiguration serverConfig) {

            this.partitionsDB = partitionsDB;
            this.serverConfig = serverConfig;
            ReliableBroadcastLayer.Instance.RegisterSelfId(this.serverConfig.ServerId);
        }

        public void Bind() {
            ReliableBroadcastLayer.Instance.BindReadHandler(OnReadRequest);
            ReliableBroadcastLayer.Instance.BindWriteHandler(OnWriteRequest);
            ReliableBroadcastLayer.Instance.BindListServerHandler(OnListServerRequest);
            ReliableBroadcastLayer.Instance.BindWriteMessageHandler(OnBroadcastWriteMessage);
            ReliableBroadcastLayer.Instance.BindStatusHandler(OnStatus);

            ReliableBroadcastLayer.Instance.BindJoinPartitionHandler(OnJoinPartitionRequest);
            ReliableBroadcastLayer.Instance.BindLookupMasterHandler(TryGetMasterUrl);
            ReliableBroadcastLayer.Instance.BindListPartitionsHandler(ListPartitionsWithServerIds);
        }

        /*
         * Handle read request from client
         */
        public (string, ImmutableVectorClock) OnReadRequest(ReadArguments arguments) {
            WaitHappensBeforeTimestamp(arguments.PartitionId, arguments.Timestamp);

            lock (this) {
                store.Read(arguments.PartitionId, arguments.ObjectId, out string value);

                // Should always have timestamp for partition, because should already be registered
                Conditions.AssertState(
                    timestamps.TryGetValue(arguments.PartitionId, out MutableVectorClock timestamp));
            
                return (value, timestamp.ToImmutable());
            }
        }

        /*
         * Handle write request from client
         */
        public ImmutableVectorClock OnWriteRequest(WriteArguments arguments) {
            ImmutableVectorClock timestampToBroadcast;

            WaitHappensBeforeTimestamp(arguments.PartitionId, arguments.Timestamp);
            
            lock(this) {
                Conditions.AssertState(
                    timestamps.TryGetValue(arguments.PartitionId, out MutableVectorClock timestamp));
                
                timestamp.Increment(serverConfig.ServerId);
                timestampToBroadcast = timestamp.ToImmutable();
            }

            // This waits for deliver and executes write
            ReliableBroadcastLayer.Instance.BroadcastWrite(
                arguments.PartitionId,
                arguments.ObjectId,
                arguments.ObjectValue,
                timestampToBroadcast);

            return timestampToBroadcast;
        }

        /*
         * Handle list server request from client
         */
        public (IEnumerable<StoredObjectDto>, IEnumerable<PartitionTimestampDto>) OnListServerRequest() {
            lock(this) {
                IEnumerable<PartitionTimestampDto> partitionTimestampDtos = timestamps.Select(pair =>{
                    return new PartitionTimestampDto {
                        PartitionId = pair.Key,
                        PartitionTimestamp = pair.Value.ToImmutable()
                    };
                });

                return (store.ListObjects(), partitionTimestampDtos);
            }
        }

        /*
         * Handle write message from the network
         */
        public void OnBroadcastWriteMessage(BroadcastWriteMessage message) {
            lock(this) {
                Conditions.AssertState(
                    timestamps.TryGetValue(message.PartitionId, out MutableVectorClock timestamp));

                // More recent update should update value
                if (VectorClock.HappensBefore(timestamp, message.ReplicaTimestamp)) {
                    // Force write
                    store.Write(message.PartitionId, message.Key, message.Value, message.WriteServerId, true);
                    timestamp.Merge(message.ReplicaTimestamp);
                }
                // Concurrent operations (keep value with smaller server id)
                else if (!VectorClock.HappensAfter(timestamp, message.ReplicaTimestamp)) {
                    // Do not force write. Only update if smaller server id so that replicas converge values
                    store.Write(message.PartitionId, message.Key, message.Value, message.WriteServerId, false);
                    timestamp.Merge(message.ReplicaTimestamp);
                }
            }
        }

        public void OnJoinPartitionRequest(JoinPartitionArguments arguments) {
            string partitionId = arguments.PartitionId;
            partitionsDB.JoinPartition(arguments);
            partitionsDB.TryGetPartition(partitionId, out ImmutableHashSet<string> serverIds);
            
            // Only register partitions that the server belongs to for broadcast
            if (serverIds.Contains(serverConfig.ServerId)) {
                timestamps.TryAdd(partitionId, MutableVectorClock.Empty());
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

        private void WaitHappensBeforeTimestamp(string partitionId, ImmutableVectorClock otherTimestamp) {
            lock(timestamps) {
                Conditions.AssertState(
                    timestamps.TryGetValue(partitionId, out MutableVectorClock timestamp));
                
                while (VectorClock.HappensBefore(timestamp, otherTimestamp)) {
                    Monitor.Wait(timestamps);
                }
            }
        }

        private void MergeTimestamp(string partitionId, ImmutableVectorClock otherTimestamp) {
            lock(timestamps) {
                Conditions.AssertState(
                    timestamps.TryGetValue(partitionId, out MutableVectorClock timestamp));
                
                timestamp.Merge(otherTimestamp);
                Monitor.PulseAll(timestamps);
            }
        }
    }
}
