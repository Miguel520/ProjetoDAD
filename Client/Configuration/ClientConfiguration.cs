using Common.Utils;

namespace Client.Configuration {
    class ClientConfiguration {
        public ClientConfiguration(
            string username,
            string host,
            int port,
            string script) {

            Username = username;
            Host = host;
            Port = port;
            Script = script;
        }

        public string Username { get; }
        public string Host { get; }
        public int Port { get; }
        public string Script { get; }

        public string Url {
            get {
                return HttpURLs.FromHostAndPort(Host, Port);
            }
        }
    }
}
