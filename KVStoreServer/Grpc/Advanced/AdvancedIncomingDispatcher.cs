using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Base;

namespace KVStoreServer.Grpc.Advanced {
    public class AdvancedIncomingDispatcher : BaseIncomingDispatcher {
        public AdvancedIncomingDispatcher(ServerConfiguration serverConfig) 
            : base(serverConfig) { }
    }
}
