using System.Threading.Tasks;

namespace KVStoreServer.Replication {

    /*
     * Interface defines technology agnostic methods 
     * required for a connection that support replication
     */
    public abstract class IReplicationConnection {

        public IReplicationConnection(string url) {
            Url = url;
        }

        public string Url { get; }

        public abstract Task Lock(string partitionId, string objectId);

        public abstract Task Write(string partitionId, string objectId, string objectValue);
    }
}
