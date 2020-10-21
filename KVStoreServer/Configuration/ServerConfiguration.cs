using Common.Utils;

namespace KVStoreServer.Configuration {
    public class ServerConfiguration {
        public ServerConfiguration(int serverId, string host, int port) {
            ServerId = serverId;
            Host = host;
            Port = port;
        }

        public int ServerId { get; }
        public string Host { get; }
        public int Port { get; }
        public string Url { get { return HttpURLs.FromHostAndPort(Host, Port); } }
    }
}
