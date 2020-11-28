using KVStoreServer.Broadcast;
using KVStoreServer.Replication.Base;

namespace KVStoreServer.Replication.Advanced {
    public class AdvancedPartitionsDB : BasePartitionsDB {

        public AdvancedPartitionsDB(string selfId, string selfUrl)
            : base(selfId, selfUrl) {}

        protected override bool RegisterServer(string serverId, string serverUrl) {
            return ReliableBroadcastLayer.Instance.RegisterServer(serverId, serverUrl);
        }

        protected override bool TryGetServer(string serverId, out string serverUrl) {
            return ReliableBroadcastLayer.Instance.TryGetServer(serverId, out serverUrl);
        }
    }
}
