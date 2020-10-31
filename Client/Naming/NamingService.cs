
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Client.Naming {
    public class NamingService {

        private readonly ImmutableList<string> nameServersUrls;
        private readonly Dictionary<int, string> knownServers;
        private readonly Dictionary<string, string> knownMasters;
        private readonly ImmutableDictionary<string, ImmutableHashSet<int>> partitions;

        public ImmutableDictionary<string, ImmutableHashSet<int>> Partitions { 
            get {
                return partitions;
            } 
        }

        public NamingService(ImmutableList<string> nameServersUrls) {
            this.nameServersUrls = nameServersUrls;
            knownServers = new Dictionary<int, string>();
            knownMasters = new Dictionary<string, string>();
            foreach (string nameServerUrl in nameServersUrls) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                if (connection.ListPartitions(out partitions)) {
                    break;
                }
            }
        }

        public bool Lookup(int id, out string url) {
            if (knownServers.TryGetValue(id, out url)) return true;

            foreach (string nameServerUrl in nameServersUrls) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                if (connection.Lookup(id, out url)) {
                    knownServers.Add(id, url);
                    break;
                }
            }

            return (url != null);
        }

        public bool LookupMaster(string partitionName, out string url) {
            if (knownMasters.TryGetValue(partitionName, out url)) return true;

            foreach (string nameServerUrl in nameServersUrls) {
                NamingServiceConnection connection =
                    new NamingServiceConnection(nameServerUrl);

                if (connection.LookupMaster(partitionName, out url)) {
                    knownMasters.Add(partitionName, url);
                    break;
                }
            }

            return (url != null);
        }
    }
}
