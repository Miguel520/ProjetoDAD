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

        // One timestamp for each partition
        private readonly ConcurrentDictionary<string, MutableVectorClock> timestamps =
            new ConcurrentDictionary<string, MutableVectorClock>();

        private readonly AdvancedPartitionedKVS store = new AdvancedPartitionedKVS();

        // One write lock for each partition
        private readonly ConcurrentDictionary<string, object> writeLocks = 
            new ConcurrentDictionary<string, object>();

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
            timestamps.TryGetValue(arguments.PartitionId, out MutableVectorClock timestamp);
            lock (timestamp) {
                while (VectorClock.HappensBefore(timestamp, arguments.Timestamp)) {
                    Monitor.Wait(timestamp);
                }

                store.Read(arguments.PartitionId, arguments.ObjectId, out string value);
            
                return (value, timestamp.ToImmutable());
            }
        }

        /*
         * Handle write request from client
         */
        public ImmutableVectorClock OnWriteRequest(WriteArguments arguments) {
            ImmutableVectorClock timestampToBroadcast;

            timestamps.TryGetValue(arguments.PartitionId, out MutableVectorClock timestamp);

            lock (timestamp) {
                while (VectorClock.HappensBefore(timestamp, arguments.Timestamp)) {
                    Monitor.Wait(timestamp);
                }
            }

            writeLocks.TryGetValue(arguments.PartitionId, out object writeLock);

            lock(writeLock) {

                MutableVectorClock mutBroadcast = MutableVectorClock.CopyOf(timestamp);
                mutBroadcast.Increment(serverConfig.ServerId);
                timestampToBroadcast = mutBroadcast.ToImmutable();

                // This waits for deliver and executes write
                ReliableBroadcastLayer.Instance.BroadcastWrite(
                    arguments.PartitionId,
                    arguments.ObjectId,
                    arguments.ObjectValue,
                    timestampToBroadcast);
            }

            return timestampToBroadcast;
        }

        /*
         * Handle list server request from client
         */
        public (IEnumerable<StoredObjectDto>, IEnumerable<PartitionTimestampDto>) OnListServerRequest() {
            lock(timestamps) {
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
         * Handle write message from the broadcast layer (already broadcasted, can be applied)
         */
        public void OnBroadcastWriteMessage(BroadcastWriteMessage message) {

            timestamps.TryGetValue(message.PartitionId, out MutableVectorClock timestamp);
            lock (timestamp) {
                // More recent update, should update value
                if (VectorClock.HappensBefore(timestamp, message.ReplicaTimestamp)) {
                    // Force write
                    store.Write(message.PartitionId, message.Key, message.Value, message.WriteServerId, true);
                    timestamp.Merge(message.ReplicaTimestamp);
                    Monitor.PulseAll(timestamp);
                }
                // Concurrent operations (keep value with smaller server id)
                else if (!VectorClock.HappensAfter(timestamp, message.ReplicaTimestamp)) {
                    // Do not force write. Only update if smaller server id so that replicas converge values
                    store.Write(message.PartitionId, message.Key, message.Value, message.WriteServerId, false);
                    timestamp.Merge(message.ReplicaTimestamp);
                    Monitor.PulseAll(timestamp);
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
                writeLocks.TryAdd(partitionId, new object());
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
    }
}
