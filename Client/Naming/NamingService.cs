using System.Collections.Immutable;
using System.Collections.Generic;
using Common.Exceptions;
using System;

namespace Client.Naming {
    public class NamingService {

        private readonly HashSet<string> nameServersUrls;
        private readonly Dictionary<string, string> knownServers;
        private readonly Dictionary<string, string> knownMasters;
        private readonly ImmutableDictionary<string, ImmutableHashSet<string>> partitions;
        private readonly HashSet<string> crashedUrls;

        public ImmutableDictionary<string, ImmutableHashSet<string>> Partitions { 
            get {
                return partitions;
            } 
        }

        public NamingService(ImmutableList<string> receivedNameServersUrls) {
            nameServersUrls = new HashSet<string>(receivedNameServersUrls);
            knownServers = new Dictionary<string, string>();
            knownMasters = new Dictionary<string, string>();
            crashedUrls = new HashSet<string>();
            partitions = ImmutableDictionary.Create<string, ImmutableHashSet<string>>();
            foreach (string nameServerUrl in receivedNameServersUrls) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                try {
                    if (connection.ListPartitions(out partitions)) {
                        break;
                    }
                }
                catch (ReplicaFailureException) {
                    // If already unavailable then remove
                    Console.WriteLine(
                        "[{0}] Replica {1} unavailable",
                        DateTime.Now.ToString("HH:mm:ss"),
                        nameServerUrl);
                    nameServersUrls.Remove(nameServerUrl);
                }
            }
        }

        /*
         * Lookup for the url of the server with the given id
         */
        public bool Lookup(string serverId, out string serverUrl) {
            if (knownServers.TryGetValue(serverId, out serverUrl)) {
                // Check if crashed
                if (crashedUrls.Contains(serverUrl)) {
                    serverUrl = null;
                    return false;
                }
                return true;
            }

            List<string> crashedServers = new List<string>();

            foreach (string nameServerUrl in nameServersUrls) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                try {
                    if (connection.Lookup(serverId, out serverUrl)) {
                        knownServers.Add(serverId, serverUrl);
                        break;
                    }
                }
                catch (ReplicaFailureException) {
                    // If nameServer unavailable then add for removal
                    // Cant instantly remove because alters nameServersUrls
                    Console.WriteLine(
                        "[{0}] Replica {1} unavailable",
                        DateTime.Now.ToString("HH:mm:ss"),
                        nameServerUrl);
                    crashedServers.Add(nameServerUrl);
                }
            }

            crashedServers.ForEach(AddCrashed);

            return (serverUrl != null);
        }

        public bool LookupMaster(string partitionId, out string masterUrl) {
            if (knownMasters.TryGetValue(partitionId, out masterUrl)) {
                // Check if crashed
                if (crashedUrls.Contains(masterUrl)) {
                    masterUrl = null;
                    return false;
                }
                return true;
            }

            List<string> crashedServers = new List<string>();

            foreach (string nameServerUrl in nameServersUrls) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                try {
                    if (connection.LookupMaster(partitionId, out masterUrl)) {
                        knownMasters.Add(partitionId, masterUrl);
                        break;
                    }
                }
                catch (ReplicaFailureException) {
                    // If nameServer unavailable then add for removal
                    // Cant instantly remove because alters nameServersUrls
                    Console.WriteLine(
                        "[{0}] Replica {1} unavailable",
                        DateTime.Now.ToString("HH:mm:ss"),
                        nameServerUrl);
                    crashedServers.Add(nameServerUrl);
                }
            }

            crashedServers.ForEach(AddCrashed);

            return (masterUrl != null);
        }

        public void AddCrashed(string serverUrl) {
            nameServersUrls.Remove(serverUrl);
            crashedUrls.Add(serverUrl);
        }
    }
}
