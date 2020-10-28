using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

using KVStoreServer.Communications;
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

        public ReplicationService(
            PartitionsDB partitionsDB,
            IReplicationConnectionFactory factory) {

            this.partitionsDB = partitionsDB;
            this.factory = factory;
        }

        public void Write(WriteArguments arguments) {

            // Allow only one read at a time
            lock (writeLock) {
                // Get partition replicas urls
                partitionsDB.TryGetPartition(arguments.PartitionName, out ImmutableHashSet<int> partitionIds);

                IEnumerable<IReplicationConnection> partitionConnections = 
                    partitionIds.Select(serverId => {
                        partitionsDB.TryGetUrl(serverId, out string url);
                        return url;
                    })
                    .Select(url => factory.ForUrl(url));

                // Lock object in all replicas
                Task[] tasks = partitionConnections.Select(
                    con => con.Lock(arguments.PartitionName, arguments.ObjectId))
                    .ToArray();

                store.Lock(
                    arguments.PartitionName,
                    arguments.ObjectId);

                Task.WaitAll(tasks);
                // Write object in all replicas
                tasks = partitionConnections.Select(
                    con => con.Write(
                        arguments.PartitionName, 
                        arguments.ObjectId, 
                        arguments.ObjectValue))
                    .ToArray();

                Task.WaitAll(tasks);

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
    }
}
