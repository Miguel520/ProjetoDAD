using KVStoreServer.Events;
using KVStoreServer.Grpc;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;

namespace KVStoreServer.Naming {
    class NamingServiceLayer {

        public static event EventHandler<IdFailureEventArgs> ReplicaFailureEvent;

        // Mappings for server ids and urls
        private readonly ConcurrentDictionary<string, string> urls =
            new ConcurrentDictionary<string, string>();

        private NamingServiceLayer() {
            GrpcMessageLayer.ReplicaFailureEvent += OnReplicaFailure;
        }

        public static NamingServiceLayer Instance { get; } = new NamingServiceLayer();

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

        public async Task Lock(
            string serverId,
            string partitionId,
            string objectId) {

            if (urls.TryGetValue(serverId, out string serverUrl)) {
                await GrpcMessageLayer.Instance.Lock(
                    serverUrl,
                    partitionId,
                    objectId);
            }
        }

        public async Task Write(
            string serverId,
            string partitionId,
            string objectId,
            string objectValue) {

            if (urls.TryGetValue(serverId, out string serverUrl)) {
                await GrpcMessageLayer.Instance.Write(
                    serverUrl,
                    partitionId,
                    objectId,
                    objectValue);
            }
        }

        public async Task Ping(
            string serverId) {

            if (urls.TryGetValue(serverId, out string serverUrl)) {
                await GrpcMessageLayer.Instance.Ping(serverUrl);
            }
        }

        private void OnReplicaFailure(object sender, UrlFailureEventArgs args) {
            lock (urls) {
                string serverUrl = args.Url;
                string serverId = urls.First(pair => pair.Value.Equals(serverUrl)).Key;
                urls.TryRemove(serverId, out _);
                ReplicaFailureEvent?.Invoke(
                    this,
                    new IdFailureEventArgs { Id = serverId });
            }
        }
    }
}
