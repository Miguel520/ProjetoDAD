
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Client.Naming {
    public class NamingService {

        private readonly ImmutableList<string> nameServersUrls;
        private readonly Dictionary<string, string> knownServers;
        private readonly Dictionary<string, string> knownMasters;
        private readonly ImmutableDictionary<string, ImmutableHashSet<string>> partitions;

        public ImmutableDictionary<string, ImmutableHashSet<string>> Partitions { 
            get {
                return partitions;
            } 
        }

        public NamingService(ImmutableList<string> nameServersUrls) {
            this.nameServersUrls = nameServersUrls;
            knownServers = new Dictionary<string, string>();
            knownMasters = new Dictionary<string, string>();
            foreach (string nameServerUrl in nameServersUrls) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                if (connection.ListPartitions(out partitions)) {
                    break;
                }
            }
        }

        /*
         * Lookup for the url of the server with the given id
         */
        public bool Lookup(string serverId, out string serverUrl) {
            if (knownServers.TryGetValue(serverId, out serverUrl)) return true;

            foreach (string nameServerUrl in nameServersUrls) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                if (connection.Lookup(serverId, out serverUrl)) {
                    knownServers.Add(serverId, serverUrl);
                    break;
                }
            }

            return (serverUrl != null);
        }

        public bool LookupMaster(string partitionId, out string masterUrl) {
            if (knownMasters.TryGetValue(partitionId, out masterUrl)) return true;

            foreach (string nameServerUrl in nameServersUrls) {
                Console.WriteLine(nameServerUrl);
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                if (connection.LookupMaster(partitionId, out masterUrl)) {
                    knownMasters.Add(partitionId, masterUrl);
                    break;
                }
            }

            return (masterUrl != null);
        }
    }
}
