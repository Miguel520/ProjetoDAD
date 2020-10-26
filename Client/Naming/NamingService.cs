
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Client.Naming {
    class NamingService {

        private readonly Dictionary<int, string> knownServers;
        private readonly ImmutableList<string> nameServersUrls;

        public NamingService(ImmutableList<string> nameServersUrls) {
            knownServers = new Dictionary<int, string>();
            this.nameServersUrls = nameServersUrls;
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
    }
}
