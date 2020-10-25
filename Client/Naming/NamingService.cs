
using System.Collections.Generic;

namespace Client.Naming {
    class NamingService {

        private readonly Dictionary<int, string> knownServers;
        private readonly string serverHost;
        private readonly int serverPort;

        public NamingService(string host, int port) {
            knownServers = new Dictionary<int, string>();
            serverHost = host;
            serverPort = port;
        }

        public bool Lookup(int id, out string url) {
            if (knownServers.TryGetValue(id, out url)) return true;

            NamingServiceConnection connection =
                new NamingServiceConnection(serverHost, serverPort);

            url = connection.Lookup(id);
            return (url != null);
        }
    }
}
