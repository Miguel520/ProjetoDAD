using Common.Utils;
using System;
using System.IO;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using KVStoreServer.Grpc.Base;

namespace KVStoreServer.Replication.Simple {

    /*
     * Class to store system partitions as well as server ids and urls
     * The partitions are immutable and only defined once
     */
    public class PartitionsDB {

        // Mappings for partition names and server ids
        private readonly ConcurrentDictionary<string, HashSet<string>> partitions =
            new ConcurrentDictionary<string, HashSet<string>>();

        // Mappings for partitions ids and master servers
        private readonly ConcurrentDictionary<string, string> partitionMasters =
            new ConcurrentDictionary<string, string>();

        private readonly string selfId;

        public PartitionsDB(string selfId, string selfUrl) {
            FailureDetectionLayer.Instance.RegisterServer(selfId, selfUrl);
            this.selfId = selfId;
        }

        public bool ConfigurePartitions(string filename) {

            TextReader file = GetFileStreamConfig(filename);
            string line = file.ReadLine();

            string[] parts = line.Split(" ");

            if (parts.Length != 2
                || !int.TryParse(parts[0], out int nServers)
                || !int.TryParse(parts[1], out int nPartitions)) {
                return false;
            }

            for (int i = 0; i < nServers; i++) {
                line = file.ReadLine();
                parts = line.Split(",");

                if (parts.Length != 2) return false;
                FailureDetectionLayer.Instance.RegisterServer(parts[0], parts[1]);

            }

            for (int i = 0; i < nPartitions; i++) {
                line = file.ReadLine();
                parts = line.Split(",");

                if (parts.Length < 2) return false;
                string partitionId = parts[0];
                string masterId = parts[1];

                partitionMasters.TryAdd(partitionId, masterId);

                HashSet<string> partition = new HashSet<string>();
                for (int j = 1; j < parts.Length; j++) {
                    if (!FailureDetectionLayer.Instance.TryGetServer(parts[j], out string _)) {
                        Console.WriteLine("Server id {0} does not exist.", parts[j]);
                        return false;
                    }
                    partition.Add(parts[j]);
                }
                partitions.TryAdd(partitionId, partition);
            }

            if (file.ReadLine() != null)
                Console.WriteLine(
                    "Bad File: only read {0} lines, {1} servers and {2} partitions.",
                    nServers + nPartitions + 1,
                    nServers,
                    nPartitions);

            //To verify the partitions and servers
            foreach ((string partitionId, HashSet<string> servers) in partitions) {
                Console.WriteLine("Partition: {0}", partitionId);
                foreach (string s in servers) {
                    Console.WriteLine("server id: {0}", s);
                }
            }
            return true;
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
                    bool alreadyExists = FailureDetectionLayer.Instance.TryGetServer(
                        serverId,
                        out string currentUrl);

                    // Url mapping must not exist or be the same as before
                    Conditions.AssertArgument(
                        !alreadyExists || currentUrl.Equals(serverUrl));
                }

                // The following operations should never raise an error
                Conditions.AssertState(partitions.TryAdd(partitionId, partition));
                Conditions.AssertState(partitionMasters.TryAdd(partitionId, masterId));

                // Insert servers correspondence
                foreach ((string serverId, string serverUrl) in members) {
                    Conditions.AssertState(
                        FailureDetectionLayer.Instance.RegisterServer(serverId, serverUrl));
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

        private HashSet<string> BuildPartition(IEnumerable<Tuple<string, string>> members) {
            HashSet<string> partition = new HashSet<string>();
            foreach ((string serverId, _) in members) {
                partition.Add(serverId);
            }
            return partition;
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

            isMaster = serverId == selfId;

            return true;
        }

        private static TextReader GetFileStreamConfig(string filename) {
            Console.WriteLine(
                "[{0}] Reading Configuration from {1}",
                DateTime.Now.ToString("HH:mm:ss"),
                filename);

            string rootDirectory = AppDomain.CurrentDomain.BaseDirectory;
            string configDirectory = rootDirectory + "..\\..\\..\\ConfigFiles\\";
            try {
                return new StreamReader(configDirectory + filename);
            }
            catch (FileNotFoundException) {
                Console.Error.WriteLine("{0}: File not found", configDirectory + filename);
                Environment.Exit(1);
                return null;
            }
        }
    }
}
