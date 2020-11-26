using KVStoreServer.Grpc.Base;
using System.Collections.Concurrent;
using System.Linq;

namespace KVStoreServer.Naming {

    public delegate void IdFailureHandler(string serverId);
    public abstract class BaseNamingServiceLayer {

        private IdFailureHandler failureHandler = null;

        // Mappings for server ids and urls
        private readonly ConcurrentDictionary<string, string> urls =
            new ConcurrentDictionary<string, string>();

        protected BaseNamingServiceLayer() {
            GetGrpcLayer().BindLookup(TryGetServer);
            GetGrpcLayer().BindFailureHandler(OnReplicaFailure);
        }

        public bool RegisterServer(
            string serverId,
            string serverUrl) {

            string finalServerUrl = urls.GetOrAdd(serverId, serverUrl);
            // If first time adding then finalServerUrl is serverUrl and they are equal
            // If not first time then finalServerUrl is the value before adding
            // and it must be equal to serverUrl
            return finalServerUrl == serverUrl;
        }

        /*
         * Returns true if a server with the given id exists and url is set to the server url,
         * otherwise returns false and url is set to null
         */
        public bool TryGetServer(
            string serverId,
            out string serverUrl) {

            return urls.TryGetValue(serverId, out serverUrl);
        }

        public void BindLookupMasterHandler(LookupMasterHandler handler) {
            GetGrpcLayer().BindLookupMasterHandler(handler);
        }

        public void BindListPartitionsHandler(ListPartitionsHandler handler) {
            GetGrpcLayer().BindListPartitionsHandler(handler);
        }

        public void BindJoinPartitionHandler(JoinPartitionHandler handler) {
            GetGrpcLayer().BindJoinPartitionHandler(handler);
        }

        public void BindStatusHandler(StatusHandler handler) {
            GetGrpcLayer().BindStatusHandler(handler);
        }

        public void BindFailureHandler(IdFailureHandler handler) {
            failureHandler = handler;
        }

        protected abstract BaseGrpcMessageLayer GetGrpcLayer();

        private void OnReplicaFailure(string serverUrl) {
            lock (urls) {
                string serverId = urls.First(pair => pair.Value.Equals(serverUrl)).Key;
                failureHandler?.Invoke(serverId);
            }
        }
    }
}
