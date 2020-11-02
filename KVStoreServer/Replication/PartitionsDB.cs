using Common.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace KVStoreServer.Replication {

    /*
     * Class to store system partitions as well as server ids and urls
     * The partitions are immutable and only defined once
     */
    public class PartitionsDB {

        // Mappings for partition names and server ids
        private readonly ConcurrentDictionary<string, HashSet<int>> partitions =
            new ConcurrentDictionary<string, HashSet<int>>();

        // Mappings for server ids and urls
        private readonly ConcurrentDictionary<int, string> urls =
            new ConcurrentDictionary<int, string>();

        // Mappings for partitions ids and master servers
        private readonly ConcurrentDictionary<string, int> partitionMasters =
            new ConcurrentDictionary<string, int>();

        private int selfId;

        public PartitionsDB(int selfId, string selfUrl) {
            urls.TryAdd(selfId, selfUrl);
            this.selfId = selfId;
        }

        /*
         * Creates a partition with the given name. Also stores all server ids and urls
         * Throws ArgumentException if a partition with the given name already exists or 
         *                          if a url for an existing id is diferent from the previous id or
         *                          if partition does not have master id
         * Throws InvalidOperationException if the state of the object is incorrect
         *                                  (should never happen)
         */
        public void AddPartition(
            string name,
            IEnumerable<Tuple<int, string>> members,
            int masterId) {

            HashSet<int> partition = BuildPartition(members);
            lock(this) {
                
                Conditions.AssertArgument(!partitions.ContainsKey(name));
                Conditions.AssertArgument(partition.Contains(masterId));

                foreach ((int serverId, string serverUrl) in members) {
                    bool alreadyExists = urls.TryGetValue(
                        serverId,
                        out string currentUrl);
                    
                    // Url mapping must not exist or be the same as before
                    Conditions.AssertArgument(
                        !alreadyExists || currentUrl.Equals(serverUrl));
                }

                // The following operations should never raise an error
                Conditions.AssertState(partitions.TryAdd(name, partition));
                Conditions.AssertState(partitionMasters.TryAdd(name, masterId));

                // Insert servers correspondence
                foreach ((int serverId, string serverUrl) in members) {
                    string addValue = urls.GetOrAdd(serverId, serverUrl);
                    // If first time adding then addValue is serverUrl and they are equal
                    // If not first time then addValue is the value before adding
                    // and it must be equal to serverUrl
                    Conditions.AssertState(addValue.Equals(serverUrl));
                }
            }
        }


        /*
         * Returns true if a partition with the given name exists and partition is set to the value,
         * otherwise returns false and partition is set to null
         */
        public bool TryGetPartition(string partitionName, out ImmutableHashSet<int> partition) {
            if (partitions.TryGetValue(partitionName, out HashSet<int> storedPartition)) {
                partition = ImmutableHashSet.CreateRange(storedPartition);
                return true;
            }
            else {
                partition = null;
                return false;
            }
        }

        /*
         * Returns true if a server with the given id exists and url is set to the server url,
         * otherwise returns false and url is set to null
         */
        public bool TryGetUrl(int serverId, out string url) {
            return urls.TryGetValue(serverId, out url);
        }

        /*
         * Returns true if a partition with the given name exists and masterId is set to the value,
         * otherwise returns false and masterId is set to 0
         */
        public bool TryGetMaster(string partitionName, out int masterId) {
            return partitionMasters.TryGetValue(partitionName, out masterId);
        }

        public ImmutableList<string> ListPartitions() {
            lock(partitions) {
                return ImmutableList.ToImmutableList(partitions.Keys);
            }
        }

        public ImmutableList<PartitionServersDto> ListPartitionsWithServerIds() {
            lock (partitions) {
                List<PartitionServersDto> list = new List<PartitionServersDto>();
                foreach (var partition in partitions) {
                    PartitionServersDto part = new PartitionServersDto {
                        PartitionName = partition.Key,
                        ServerIds = new HashSet<int>()
                    };
                    foreach (var server in partition.Key) {
                        part.ServerIds.Add(server);
                    }
                    list.Add(part);
                }
                return list.ToImmutableList();
            }
        }        

        private HashSet<int> BuildPartition(IEnumerable<Tuple<int, string>> members) {
            HashSet<int> partition = new HashSet<int>();
            foreach ((int serverId, _) in members) {
                partition.Add(serverId);
            }
            return partition;
        }

        public bool IsPartitionMaster(string partitionName, out bool isMaster) {
            isMaster = false;
            bool success = partitionMasters.TryGetValue(
                partitionName,
                out int serverId);

            if (!success) {
                Console.WriteLine($"Error in finding partition {partitionName}");
                return false;
            }

            isMaster = serverId == selfId;

            return true;
        }

        /*
         * Removes server with given url from every partition, or correspondence
         */
        public void RemoveUrl(string serverUrl) {
            int serverId = urls.First(pair => pair.Value.Equals(serverUrl)).Key;
            string partitionName = partitionMasters.FirstOrDefault(pair => pair.Value == serverId).Key;

            // Remove from partitions
            lock (this) {
                foreach (HashSet<int> partition in partitions.Values) {
                    partition.Remove(serverId);
                }

                // Remove from correspondences
                urls.TryRemove(serverId, out _);
                // Remove from masters
                if (partitionName != null) {
                    partitionMasters.TryRemove(partitionName, out _);
                }
            }
        }
    }
}
