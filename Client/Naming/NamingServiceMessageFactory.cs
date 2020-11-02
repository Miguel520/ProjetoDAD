using Common.Protos.NamingService;

namespace Client.Naming {
    /*
     * Factory class to build messages related to PuppetMaster communication
     */
    class NamingServiceMessageFactory {
        public NamingServiceMessageFactory() {
        }

        public static LookupRequest BuildLookupRequest(string serverId) {
            return new LookupRequest {
                ServerId = serverId
            };
        }

        public static LookupMasterRequest BuildLookupMasterRequest(
            string partitionId) {

            return new LookupMasterRequest {
                PartitionId = partitionId
            };
        }
    }
}
