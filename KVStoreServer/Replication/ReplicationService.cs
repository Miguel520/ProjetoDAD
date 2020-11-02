using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Common.Exceptions;
using KVStoreServer.Communications;
using KVStoreServer.Configuration;
using KVStoreServer.Storage;

namespace KVStoreServer.Replication {

    /**
     * Class responsible for replication related operation
     * such as writing and reading operations
     */
    public class ReplicationService {

        private readonly object writeLock = new object();

        private readonly IReplicationConnectionFactory factory;
        private readonly PartitionsDB partitionsDB;
        private readonly PartitionedKeyValueStore store =
            new PartitionedKeyValueStore();
        private readonly ServerConfiguration config;

        public ReplicationService(
            PartitionsDB partitionsDB,
            IReplicationConnectionFactory factory,
            ServerConfiguration config) {

            this.partitionsDB = partitionsDB;
            this.factory = factory;
            this.config = config;
        }

        public string Read(ReadArguments arguments) {
            store.TryGet(arguments.PartitionName, arguments.ObjectId, out string value);
            return value;
        }

        public void Write(WriteArguments arguments) {

            // Allow only one read at a time
            lock (writeLock) {
                // Get partition replicas urls
                partitionsDB.TryGetPartition(arguments.PartitionName, out ImmutableHashSet<int> partitionIds);

                HashSet<IReplicationConnection> partitionConnections =
                    partitionIds.Where(id => id != config.ServerId)
                        .Select(serverId => {
                            partitionsDB.TryGetUrl(serverId, out string url);
                            return url;
                        })
                        .Select(url => factory.ForUrl(url))
                        .ToHashSet();

                if (partitionConnections.Count > 0) {
                    // Lock object in all replicas
                    Task[] tasks = partitionConnections.Select(
                        con => con.Lock(arguments.PartitionName, arguments.ObjectId))
                        .ToArray();

                    try {
                        Task.WaitAll(tasks);
                    } 
                    catch (AggregateException ex) {
                        foreach (var innerEx in ex.InnerExceptions) {
                            if (innerEx is ReplicaFailureException replicaFailureException) {
                                partitionsDB.RemoveUrl(replicaFailureException.Url);
                                // Try to write the value in the remaining replicas
                                partitionConnections = partitionConnections.Where(
                                    con => con.Url != replicaFailureException.Url)
                                    .ToHashSet();
                            }
                            else {
                                throw innerEx;
                            }
                        }
                    }
                }
                
                store.Lock(
                    arguments.PartitionName,
                    arguments.ObjectId);

                if (partitionConnections.Count > 0) {
                    // Write object in all replicas
                    Task[] tasks = partitionConnections.Select(
                        con => con.Write(
                            arguments.PartitionName,
                            arguments.ObjectId,
                            arguments.ObjectValue))
                        .ToArray();

                    try { 
                        Task.WaitAll(tasks);
                    }
                    catch (AggregateException ex) {
                        foreach (var innerEx in ex.InnerExceptions) {
                            if (innerEx is ReplicaFailureException replicaFailureException) {
                                partitionsDB.RemoveUrl(replicaFailureException.Url);
                            }
                            throw innerEx;
                        }
                    }
                }

                // Write value
                store.AddOrUpdate(
                    arguments.PartitionName,
                    arguments.ObjectId,
                    arguments.ObjectValue);
            }
        }

        /* Internal operations for replication */

        public void Lock(LockArguments arguments) {
            store.Lock(arguments.PartitionName, arguments.ObjectId);
        }

        public void WriteObject(WriteObjectArguments arguments) {
            store.AddOrUpdate(
                arguments.PartitionName,
                arguments.ObjectId,
                arguments.ObjectValue);
        }

        public void TryGetAllObjects(out List<StoredValueDto> objects) {
            store.TryGetAllObjects(out objects);
            foreach (StoredValueDto stored in objects) {
                partitionsDB.IsPartitionMaster(stored.PartitionName, out bool isMaster);
                stored.IsMaster = isMaster;
            }

        }

        public void TryGetAllObjectsThisPartition(out List<StoredValueDto> objects, ListIdsArguments arguments) {
            List<StoredValueDto> tempObjects = new List<StoredValueDto>();
            objects = new List<StoredValueDto>();
            store.TryGetAllObjects(out tempObjects);
            foreach (StoredValueDto stored in tempObjects) {
                if (stored.PartitionName.Equals(arguments.PartitionName)) {
                    objects.Add(stored);
                }
            }
        }
    }
}
