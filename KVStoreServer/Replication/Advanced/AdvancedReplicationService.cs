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
        private readonly ConcurrentDictionary<string, MutableVectorClock> valueTimestamps =
            new ConcurrentDictionary<string, MutableVectorClock>();

        private readonly AdvancedPartitionedKVS store = new AdvancedPartitionedKVS();

        // Timestamps for accepted writes that are not yet executed
        // Are sent for broadcast and will be executed when broadcasted
        // These timestamps are always equal to the correspondig timestamp in valueTimestamps
        // except for the server id entry where they can be higher
        private readonly ConcurrentDictionary<string, MutableVectorClock> emitedWritesTimestamps = 
            new ConcurrentDictionary<string, MutableVectorClock>();

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
            valueTimestamps.TryGetValue(arguments.PartitionId, out MutableVectorClock timestamp);
            lock (timestamp) {

                while (!(VectorClock.Equal(timestamp, arguments.Timestamp)
                    || VectorClock.HappensAfter(timestamp, arguments.Timestamp))) {
                    
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

            valueTimestamps.TryGetValue(arguments.PartitionId, out MutableVectorClock timestamp);

            // Wait for unreceived updates
            lock (timestamp) {
                while (VectorClock.HappensBefore(timestamp, arguments.Timestamp)) {
                    Monitor.Wait(timestamp);
                }
            }

            emitedWritesTimestamps.TryGetValue(
                arguments.PartitionId, 
                out MutableVectorClock emitedWriteTimestamp);

            // Prepare to emit one write
            lock(emitedWriteTimestamp) {
                emitedWriteTimestamp.Increment(serverConfig.ServerId);
                timestampToBroadcast = emitedWriteTimestamp.ToImmutable();
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
                IEnumerable<PartitionTimestampDto> partitionTimestampDtos = valueTimestamps.Select(pair =>{
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

            valueTimestamps.TryGetValue(message.PartitionId, out MutableVectorClock timestamp);
            emitedWritesTimestamps.TryGetValue(message.PartitionId, out MutableVectorClock emitedWriteTimestamp);
            lock (timestamp) {
                if (store.Write(message.PartitionId, message.Key, message.Value, message.WriteServerId, timestamp, message.ReplicaTimestamp)) {
                    // Update both timestamps to reflect write
                    timestamp.Merge(message.ReplicaTimestamp);
                    emitedWriteTimestamp.Merge(message.ReplicaTimestamp);
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
                valueTimestamps.TryAdd(partitionId, MutableVectorClock.Empty());
                emitedWritesTimestamps.TryAdd(partitionId, MutableVectorClock.Empty());
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
