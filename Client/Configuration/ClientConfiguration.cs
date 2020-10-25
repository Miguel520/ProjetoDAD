using Common.Utils;

namespace Client.Configuration {
    class ClientConfiguration {
        public ClientConfiguration(
            string username,
            string host,
            int port,
            string script,
            string server_host,
            int server_port) {

            Username = username;
            Host = host;
            Port = port;
            Script = script;
            ServerHost = server_host;
            ServerPort = server_port;
        }

        public string Username { get; }
        public string Host { get; }
        public int Port { get; }
        public string Script { get; }
        public string ServerHost { get; }
        public int ServerPort { get; }

        public string Url {
            get {
                return HttpURLs.FromHostAndPort(Host, Port);
            }
        }
    }
}
