using System.IO;

namespace PuppetMaster.Configuration {
    public class PMConfiguration {

        public string Host { get; set; }
        public int Port { get; set; }

        public int Version { get; set; }

        public TextReader InputSource { get; set; }
    }
}
