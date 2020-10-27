namespace KVStoreServer.Replication {
    
    /*
     * Factory to create replication connections
     */
    public interface IReplicationConnectionFactory {

        IReplicationConnection ForUrl(string url);
    }
}
