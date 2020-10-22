using Common.Utils;

namespace KVStoreServer.Configuration {
    public class ServerConfiguration {
        public ServerConfiguration(
            int serverId,
            string host,
            int port,
            int minDelay,
            int maxDelay) {

            ServerId = serverId;
            Host = host;
            Port = port;
            MinDelay = minDelay;
            MaxDelay = maxDelay;
        }

        public int ServerId { get; }
        public string Host { get; }
        public int Port { get; }
        public int MinDelay { get; }
        public int MaxDelay { get; }

        public string Url {
            get {
                return HttpURLs.FromHostAndPort(Host, Port);
            }
        }
    }
}
