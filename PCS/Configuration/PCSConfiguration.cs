using Common.Utils;

namespace PCS.Configuration {
    class PCSConfiguration {
        public PCSConfiguration(
            string host,
            int port) {

            Host = host;
            Port = port;
        }

        public string Host { get; }
        public int Port { get; }

        public string Url {
            get {
                return HttpURLs.FromHostAndPort(Host, Port);
            }
        }
    }
}
