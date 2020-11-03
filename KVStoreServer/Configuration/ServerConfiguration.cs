using Common.Utils;

namespace KVStoreServer.Configuration {
    public class ServerConfiguration {
        public ServerConfiguration(
            string serverId,
            string host,
            int port,
            int minDelay,
            int maxDelay, 
            string filename) {

            ServerId = serverId;
            Host = host;
            Port = port;
            MinDelay = minDelay;
            MaxDelay = maxDelay;
            Filename = filename;
        }

        public string ServerId { get; }
        public string Host { get; }
        public int Port { get; }
        public int MinDelay { get; }
        public int MaxDelay { get; }
        public string Filename { get; }

        public string Url {
            get {
                return HttpURLs.FromHostAndPort(Host, Port);
            }
        }
    }
}
