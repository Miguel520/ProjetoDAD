﻿using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using KVStoreServer.Events;
using KVStoreServer.Naming;

namespace KVStoreServer.Replication {

    /*
     * Layer responsible for the fast detection of server crashes
     * Also responsible for setting timeouts for the requests
     */
    public class FailureDetectionLayer {

        private static readonly int PING_DELAY = 10000;
        private static readonly int INITIAL_TIMEOUT = 60000;

        private ConcurrentDictionary<string, long> currentTimeouts =
            new ConcurrentDictionary<string, long>();

        private string selfId;

        private FailureDetectionLayer() {
            NamingServiceLayer.ReplicaFailureEvent += OnReplicaFailure;
            Task.Run(() => PingAliveServers());
        }

        public static FailureDetectionLayer Instance { get; } = new FailureDetectionLayer();

        public void RegisterSelfId(string serverId) {
            selfId = serverId;
        }

        public bool RegisterServer(
            string serverId,
            string serverUrl) {
            
            if (NamingServiceLayer.Instance.RegisterServer(serverId, serverUrl)) {
                currentTimeouts.TryAdd(serverId, INITIAL_TIMEOUT);
                return true;
            }
            return false;
        }

        public bool TryGetServer(
            string serverId,
            out string serverUrl) {

            return NamingServiceLayer.Instance.TryGetServer(serverId, out serverUrl);
        }

        public async Task Lock(
            string serverId,
            string partitionId,
            string objectId) {

            await NamingServiceLayer.Instance.Lock(serverId, partitionId, objectId);
        }

        public async Task Write(
            string serverId,
            string partitionId,
            string objectId,
            string objectValue) {

            await NamingServiceLayer.Instance.Write(serverId, partitionId, objectId, objectValue);
        }

        private async void PingAliveServers() {
            while(true) {
                //Ping all servers
                Task[] tasks = currentTimeouts.Keys.Where(id => selfId != id).Select(PingSingleServer).ToArray();
                Task.WaitAll(tasks);
                await Task.Delay(PING_DELAY);
            }
        }

        private async Task PingSingleServer(string serverId) {
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            await NamingServiceLayer.Instance.Ping(serverId);
            stopWatch.Stop();
            long timeEllapsed = stopWatch.ElapsedMilliseconds;
            // TODO: Update value with walking average
        }

        public void OnReplicaFailure(object sender, IdFailureEventArgs args) {
            string serverId = args.Id;
            currentTimeouts.TryRemove(serverId, out _);
        }
    }
}