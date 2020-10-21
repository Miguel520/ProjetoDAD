using Common.Utils;
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

        private readonly ConcurrentDictionary<string, ImmutableHashSet<int>> partitions =
            new ConcurrentDictionary<string, ImmutableHashSet<int>>();

        private readonly ConcurrentDictionary<int, string> urls =
            new ConcurrentDictionary<int, string>();

        public PartitionsDB() {}

        /*
         * Creates a partition with the given name. Also stores all server ids and urls
         * Throws ArgumentException if a partition with the given name already exists or if
         * a url for an existing id is diferent from the previous id
         */
        public void AddPartition(string name, IEnumerable<Tuple<int, string>> members) {
            ImmutableHashSet<int> partition = BuildPartition(members);
            lock(this) {
                Conditions.AssertTrue(partitions.TryAdd(name, partition),
                                      () => throw new ArgumentException());

                foreach ((int serverId, string serverUrl) in members) {
                    string addValue = urls.GetOrAdd(serverId, serverUrl);
                    // If first time adding then addValue is serverUrl and they are equal
                    // If not first time then addValue is the value before adding and it must be equal to serverUrl
                    Conditions.AssertTrue(addValue.Equals(serverUrl),
                                          () => throw new ArgumentException());
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

        private ImmutableHashSet<int> BuildPartition(IEnumerable<Tuple<int, string>> members) {
            ImmutableHashSet<int>.Builder builder = ImmutableHashSet.CreateBuilder<int>();
            foreach ((int serverId, _) in members) {
                builder.Add(serverId);
            }
            return builder.ToImmutableHashSet();
        }
    }
}
