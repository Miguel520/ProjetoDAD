using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc {
    public class GrpcMessageLayer {

        public static event EventHandler<ReplicaFailureEventArgs> ReplicaFailureEvent;

        private readonly HashSet<string> crashedServers = new HashSet<string>();

        private GrpcMessageLayer() { }
        public static GrpcMessageLayer Instance { get; } = new GrpcMessageLayer();

        public async Task Lock(
            string serverUrl, 
            string partitionId, 
            string objectId) {

            lock(crashedServers) {
                if (crashedServers.Contains(serverUrl)) {
                    return;
                }
            }

            try {
                ReplicationConnection connection = new ReplicationConnection(serverUrl);
                await connection.Lock(partitionId, objectId);
            }
            catch (RpcException exception) {
                HandleRpcException(serverUrl, exception);
            }
        }

        public async Task Write(
            string serverUrl,
            string partitionId,
            string objectId,
            string objectValue) {

            lock (crashedServers) {
                if (crashedServers.Contains(serverUrl)) {
                    return;
                }
            }

            try {
                ReplicationConnection connection = new ReplicationConnection(serverUrl);
                await connection.Write(partitionId, objectId, objectValue);
            }
            catch (RpcException exception) {
                HandleRpcException(serverUrl, exception);
            }
        }

        private void HandleRpcException(string serverUrl, RpcException exception) {
            if (exception.StatusCode == StatusCode.DeadlineExceeded ||
                exception.StatusCode == StatusCode.Internal) {

                BroadcastReplicaFailure(serverUrl);
                Console.WriteLine(
                    "[{0}] Replica {1} unavailable",
                    DateTime.Now.ToString("HH:mm:ss"),
                    serverUrl);
            }
            else {
                Console.WriteLine(
                    "[{0}] Error {1} with operation on server {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    exception.StatusCode,
                    serverUrl);
            }
        }

        private void BroadcastReplicaFailure(string serverUrl) {
            lock (crashedServers) {
                crashedServers.Add(serverUrl);
            }
            ReplicaFailureEvent?.Invoke(
                this,
                new ReplicaFailureEventArgs { Url = serverUrl });
        }
    }
}
