using Common.Protos.ProcessCreation;
using System.Collections.Generic;

namespace PuppetMaster.PCS {

    /*
     * Factory class to build messages related to PCS communication
     */
    public class PCSMessageFactory {
        private PCSMessageFactory() {
        }

        public static CreateServerRequest BuildCreateServerRequest(
            string serverId,
            string host,
            int port,
            int minDelay,
            int maxDelay,
            int version) {

            return new CreateServerRequest {
                ServerId = serverId,
                Host = host,
                Port = port,
                MinDelay = minDelay,
                MaxDelay = maxDelay,
                Version = version
            };
        }

        public static CreateClientRequest BuildCreateClientRequest(
            string username,
            string host,
            int port,
            string scriptFile,
            IEnumerable<string> namingServersUrls,
            int version) {

            return new CreateClientRequest {
                Username = username,
                Host = host,
                Port = port,
                Script = scriptFile,
                NamingServersUrls = { namingServersUrls },
                Version = version
            };
        }
    }
}
