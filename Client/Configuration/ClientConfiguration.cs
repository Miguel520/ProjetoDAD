using Common.Utils;
using System.Collections.Immutable;

namespace Client.Configuration {
    class ClientConfiguration {
        public ClientConfiguration(
            string username,
            string host,
            int port,
            string script,
            ImmutableList<string> namingServersUrls) {

            Username = username;
            Host = host;
            Port = port;
            Script = script;
            NamingServersUrls = namingServersUrls;
        }

        public string Username { get; }
        public string Host { get; }
        public int Port { get; }
        public string Script { get; }
        public ImmutableList<string> NamingServersUrls { get; }

        public string Url {
            get {
                return HttpURLs.FromHostAndPort(Host, Port);
            }
        }
    }
}
