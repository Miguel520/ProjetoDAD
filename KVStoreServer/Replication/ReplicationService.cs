using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc;
using KVStoreServer.Storage;

namespace KVStoreServer.Replication {

    /**
     * Class responsible for replication related operation
     * such as writing and reading operations
     */
    public class ReplicationService {

        private readonly object writeLock = new object();

        private readonly PartitionsDB partitionsDB;
        private readonly PartitionedKeyValueStore store =
            new PartitionedKeyValueStore();
        private readonly ServerConfiguration config;

        public ReplicationService(
            PartitionsDB partitionsDB,
            ServerConfiguration config) {

            this.partitionsDB = partitionsDB;
            this.config = config;
        }

        public void Bind() {
            FailureDetectionLayer.Instance.BindReadHandler(Read);
            FailureDetectionLayer.Instance.BindWriteHandler(Write);
            FailureDetectionLayer.Instance.BindListServerHandler(ListServer);
            FailureDetectionLayer.Instance.BindLockHandler(Lock);
            FailureDetectionLayer.Instance.BindWriteObjectHandler(WriteObject);
            FailureDetectionLayer.Instance.BindStatusHandler(Status);

            FailureDetectionLayer.Instance.BindJoinPartitionHandler(JoinPartition);
            FailureDetectionLayer.Instance.BindLookupMasterHandler(TryGetMasterUrl);
            FailureDetectionLayer.Instance.BindListPartitionsHandler(ListPartitionsWithServerIds);
        }

        public string Read(ReadArguments arguments) {
            store.TryGet(arguments.PartitionId, arguments.ObjectId, out string value);
            return value;
        }

        public void Write(WriteArguments arguments) {
            // Allow only one read at a time
            lock (writeLock) {
                // Get partition replicas urls
                partitionsDB.TryGetPartition(arguments.PartitionId, out ImmutableHashSet<string> partitionIds);

                List<string> otherReplicasIds = partitionIds.Where(id => id != config.ServerId).ToList();

                if (otherReplicasIds.Count > 0) {
                    // Lock object in all replicas
                    Task[] tasks = otherReplicasIds.Select(
                        id => FailureDetectionLayer.Instance.Lock(
                            id, 
                            arguments.PartitionId, 
                            arguments.ObjectId))
                        .ToArray();

                    Task.WaitAll(tasks);
                }
                
                store.Lock(
                    arguments.PartitionId,
                    arguments.ObjectId);

                if (otherReplicasIds.Count > 0) {
                    // Write object in all replicas
                    Task[] tasks = otherReplicasIds.Select(
                        id => FailureDetectionLayer.Instance.Write(
                            id,
                            arguments.PartitionId,
                            arguments.ObjectId,
                            arguments.ObjectValue))
                        .ToArray();

                    Task.WaitAll(tasks);
                }

                // Write value
                store.AddOrUpdate(
                    arguments.PartitionId,
                    arguments.ObjectId,
                    arguments.ObjectValue);
            }
        }

        /* Internal operations for replication */

        public void Lock(LockArguments arguments) {
            store.Lock(arguments.PartitionId, arguments.ObjectId);
        }

        public void WriteObject(WriteObjectArguments arguments) {
            store.AddOrUpdate(
                arguments.PartitionId,
                arguments.ObjectId,
                arguments.ObjectValue);
        }

        public ImmutableList<StoredValueDto> ListServer() {
            store.TryGetAllObjects(out List<StoredValueDto> objects);
            foreach (StoredValueDto stored in objects) {
                partitionsDB.IsPartitionMaster(stored.PartitionId, out bool isMaster);
                stored.IsMaster = isMaster;
            }
            return objects.ToImmutableList();
        }

        public void JoinPartition(JoinPartitionArguments arguments) {
            partitionsDB.JoinPartition(arguments);
        }

        public bool TryGetMasterUrl(string partitionId, out string masterUrl) {
            masterUrl = default;
            return partitionsDB.TryGetMasterUrl(partitionId, out string masterId)
                && FailureDetectionLayer.Instance.TryGetServer(masterId, out masterUrl);
        }

        public ImmutableList<PartitionServersDto> ListPartitionsWithServerIds() {
            return partitionsDB.ListPartitionsWithServerIds();
        }

        public void Status() {
            Console.WriteLine(
                "[{0}] Server with id {1} running at {2}",
                 DateTime.Now.ToString("HH:mm:ss"),
                 config.ServerId,
                 config.Url);

            Console.WriteLine(
                "[{0}]  Partitions: {1}",
                DateTime.Now.ToString("HH:mm:ss"),
                string.Join(", ", partitionsDB.ListPartitions()));
        }
    }
}
