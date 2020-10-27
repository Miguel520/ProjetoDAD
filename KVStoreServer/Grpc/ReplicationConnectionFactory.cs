using KVStoreServer.Replication;

namespace KVStoreServer.Grpc {
    public class ReplicationConnectionFactory : IReplicationConnectionFactory {
        public IReplicationConnection ForUrl(string url) {
            return new ReplicationConnection(url);
        }
    }
}
