using Common.Protos.NamingService;

namespace Client.Naming {
    /*
     * Factory class to build messages related to PuppetMaster communication
     */
    class NamingServiceMessageFactory {
        public NamingServiceMessageFactory() {
        }

        public static LookupRequest BuildLookupRequest(int id) {
            return new LookupRequest {
                ServerId = id
            };
        }
    }
}
