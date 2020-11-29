using Common.Utils;
using KVStoreServer.Grpc.Base;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KVStoreServer.Replication.Base {
    public abstract class BasePartitionsDB {

        // Mappings for partition names and server ids
        private readonly ConcurrentDictionary<string, HashSet<string>> partitions =
            new ConcurrentDictionary<string, HashSet<string>>();

        // Mappings for partitions ids and master servers
        private readonly ConcurrentDictionary<string, string> partitionMasters =
            new ConcurrentDictionary<string, string>();

        private readonly string selfId;

        protected BasePartitionsDB(string selfId, string selfUrl) {
            RegisterServer(selfId, selfUrl);
            this.selfId = selfId;
        }

        protected ImmutableDictionary<string, ImmutableHashSet<string>> Partitions {
            get {
                return ImmutableDictionary.CreateRange(
                    partitions.Select(pair => 
                        new KeyValuePair<string, ImmutableHashSet<string>>(
                            pair.Key, 
                            pair.Value.ToImmutableHashSet())));
            }
        }

        /*
         * Creates a partition with the given name. Also stores all server ids and urls
         * Throws ArgumentException if a partition with the given name already exists or 
         *                          if a url for an existing id is diferent from the previous id or
         *                          if partition does not have master id
         * Throws InvalidOperationException if the state of the object is incorrect
         *                                  (should never happen)
         */
        public void JoinPartition(JoinPartitionArguments arguments) {

            string partitionId = arguments.PartitionId;
            IEnumerable<Tuple<string, string>> members = arguments.Members;
            string masterId = arguments.MasterId;

            HashSet<string> partition = BuildPartition(members);
            lock (this) {

                Conditions.AssertArgument(!partitions.ContainsKey(partitionId));
                Conditions.AssertArgument(partition.Contains(masterId));

                foreach ((string serverId, string serverUrl) in members) {
                    bool alreadyExists = TryGetServer(serverId, out string currentUrl);

                    // Url mapping must not exist or be the same as before
                    Conditions.AssertArgument(
                        !alreadyExists || currentUrl.Equals(serverUrl));
                }

                // The following operations should never raise an error
                Conditions.AssertState(partitions.TryAdd(partitionId, partition));
                Conditions.AssertState(partitionMasters.TryAdd(partitionId, masterId));

                // Insert servers correspondence
                foreach ((string serverId, string serverUrl) in members) {
                    Conditions.AssertState(RegisterServer(serverId, serverUrl));
                }
            }
        }

        /*
         * Returns true if a partition with the given name exists and partition is set to the value,
         * otherwise returns false and partition is set to null
         */
        public bool TryGetPartition(string partitionId, out ImmutableHashSet<string> partition) {
            if (partitions.TryGetValue(partitionId, out HashSet<string> storedPartition)) {
                partition = ImmutableHashSet.CreateRange(storedPartition);
                return true;
            }
            else {
                partition = null;
                return false;
            }
        }

        /*
         * Returns true if a partition with the given name exists and masterId is set to the value,
         * otherwise returns false and masterId is set to 0
         */
        public bool TryGetMasterUrl(string partitionId, out string masterId) {
            return partitionMasters.TryGetValue(partitionId, out masterId);
        }

        public ImmutableList<string> ListPartitions() {
            lock (partitions) {
                return partitions.Keys.ToImmutableList();
            }
        }

        public ImmutableList<PartitionServersDto> ListPartitionsWithServerIds() {
            lock (partitions) {
                List<PartitionServersDto> list = new List<PartitionServersDto>();
                foreach (var partition in partitions) {
                    PartitionServersDto part = new PartitionServersDto {
                        PartitionId = partition.Key,
                        ServerIds = new HashSet<string>()
                    };
                    foreach (string server in partition.Value) {
                        part.ServerIds.Add(server);
                    }
                    list.Add(part);
                }
                return list.ToImmutableList();
            }
        }

        public bool IsPartitionMaster(string partitionId, out bool isMaster) {
            isMaster = false;
            bool success = partitionMasters.TryGetValue(
                partitionId,
                out string serverId);

            if (!success) {
                Console.WriteLine($"Error in finding partition {partitionId}");
                return false;
            }

            isMaster = (serverId == selfId);

            return true;
        }

        private HashSet<string> BuildPartition(IEnumerable<Tuple<string, string>> members) {
            HashSet<string> partition = new HashSet<string>();
            foreach ((string serverId, _) in members) {
                partition.Add(serverId);
            }
            return partition;
        }

        protected bool TryAddPartition(string partitionId, HashSet<string> serverIds) {
            return partitions.TryAdd(partitionId, serverIds);
        }

        protected bool TryAddMaster(string partitionId, string masterId) {
            return partitionMasters.TryAdd(partitionId, masterId);
        }

        protected abstract bool TryGetServer(string serverId, out string serverUrl);

        protected abstract bool RegisterServer(string serverId, string serverUrl);

    }
}
