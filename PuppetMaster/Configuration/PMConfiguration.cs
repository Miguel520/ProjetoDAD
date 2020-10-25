using Common.Utils;
using System.IO;

namespace PuppetMaster.Configuration {
    public class PMConfiguration {

        public string Host { get; set; }
        public int Port { get; set; }

        public string Url { get { return HttpURLs.FromHostAndPort(Host, Port); } }

        public TextReader InputSource { get; set; }
    }
}
