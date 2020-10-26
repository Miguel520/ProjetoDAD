﻿using Common.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace KVStoreServer.Replication {

    /*
     * Class to store system partitions as well as server ids and urls
     * The partitions are immutable and only defined once
     */
    public class PartitionsDB {

        // Mappings for partition names and server ids
        private readonly ConcurrentDictionary<string, ImmutableHashSet<int>> partitions =
            new ConcurrentDictionary<string, ImmutableHashSet<int>>();

        // Mappings for server ids and urils
        private readonly ConcurrentDictionary<int, string> urls =
            new ConcurrentDictionary<int, string>();

        // Mappings for partitions ids and master servers
        private readonly ConcurrentDictionary<string, int> partitionMasters =
            new ConcurrentDictionary<string, int>();

        public PartitionsDB() {}

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

            ImmutableHashSet<int> partition = BuildPartition(members);
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
            return partitions.TryGetValue(partitionName, out partition);
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

        private ImmutableHashSet<int> BuildPartition(IEnumerable<Tuple<int, string>> members) {
            ImmutableHashSet<int>.Builder builder = ImmutableHashSet.CreateBuilder<int>();
            foreach ((int serverId, _) in members) {
                builder.Add(serverId);
            }
            return builder.ToImmutableHashSet();
        }
    }
}
