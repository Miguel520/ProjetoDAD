using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Common.Exceptions;
using KVStoreServer.Communications;
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

        public string Read(ReadArguments arguments) {
            store.TryGet(arguments.PartitionId, arguments.ObjectId, out string value);
            return value;
        }

        public void Write(WriteArguments arguments) {

            // Allow only one read at a time
            lock (writeLock) {
                // Get partition replicas urls
                partitionsDB.TryGetPartition(arguments.PartitionId, out ImmutableHashSet<string> partitionIds);

                HashSet<string> partitionUrls =
                    partitionIds.Where(id => id != config.ServerId)
                        .Select(serverId => {
                            partitionsDB.TryGetUrl(serverId, out string url);
                            return url;
                        })
                        .ToHashSet();

                if (partitionUrls.Count > 0) {
                    // Lock object in all replicas
                    Task[] tasks = partitionUrls.Select(
                        url => GrpcMessageLayer.Instance.Lock(
                            url, 
                            arguments.PartitionId, 
                            arguments.ObjectId))
                        .ToArray();

                    Task.WaitAll(tasks);
                }
                
                store.Lock(
                    arguments.PartitionId,
                    arguments.ObjectId);

                if (partitionUrls.Count > 0) {
                    // Write object in all replicas
                    Task[] tasks = partitionUrls.Select(
                        url => GrpcMessageLayer.Instance.Write(
                            url,
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

        public void TryGetAllObjects(out List<StoredValueDto> objects) {
            store.TryGetAllObjects(out objects);
            foreach (StoredValueDto stored in objects) {
                partitionsDB.IsPartitionMaster(stored.PartitionId, out bool isMaster);
                stored.IsMaster = isMaster;
            }

        }
    }
}
