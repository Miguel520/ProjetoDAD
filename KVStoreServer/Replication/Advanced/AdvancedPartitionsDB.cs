using KVStoreServer.Broadcast;
using KVStoreServer.Replication.Base;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;

namespace KVStoreServer.Replication.Advanced {
    public class AdvancedPartitionsDB : BasePartitionsDB {

        public AdvancedPartitionsDB(string selfId, string selfUrl)
            : base(selfId, selfUrl) {}

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
                ReliableBroadcastLayer.Instance.RegisterServer(parts[0], parts[1]);

            }

            for (int i = 0; i < nPartitions; i++) {
                line = file.ReadLine();
                parts = line.Split(",");

                if (parts.Length < 2) return false;
                string partitionId = parts[0];

                HashSet<string> partition = new HashSet<string>();
                for (int j = 1; j < parts.Length; j++) {
                    if (!ReliableBroadcastLayer.Instance.TryGetServer(parts[j], out string _)) {
                        Console.WriteLine("Server id {0} does not exist.", parts[j]);
                        return false;
                    }
                    partition.Add(parts[j]);
                }
                TryAddPartition(partitionId, partition);
            }

            if (file.ReadLine() != null)
                Console.WriteLine(
                    "Bad File: only read {0} lines, {1} servers and {2} partitions.",
                    nServers + nPartitions + 1,
                    nServers,
                    nPartitions);

            //To verify the partitions and servers
            foreach ((string partitionId, ImmutableHashSet<string> servers) in Partitions) {
                Console.WriteLine("Partition: {0}", partitionId);
                foreach (string s in servers) {
                    Console.WriteLine("server id: {0}", s);
                }
            }
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

        protected override bool RegisterServer(string serverId, string serverUrl) {
            return ReliableBroadcastLayer.Instance.RegisterServer(serverId, serverUrl);
        }

        protected override bool TryGetServer(string serverId, out string serverUrl) {
            return ReliableBroadcastLayer.Instance.TryGetServer(serverId, out serverUrl);
        }
    }
}
