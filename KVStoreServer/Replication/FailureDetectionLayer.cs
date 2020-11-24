using System;
using System.Collections.Concurrent;
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
        private static readonly int INITIAL_RTT = 15000;
        // Dont let timeout be to low
        private static readonly int MIN_TIMOUT = 5000;

        private ConcurrentDictionary<string, long> estimatedRTT =
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
                estimatedRTT.TryAdd(serverId, INITIAL_RTT);
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

            if (estimatedRTT.TryGetValue(serverId, out long rtt)) {
                await NamingServiceLayer.Instance.Lock(
                    serverId, 
                    partitionId, 
                    objectId, 
                    2 * rtt + MIN_TIMOUT);
            }
        }

        public async Task Write(
            string serverId,
            string partitionId,
            string objectId,
            string objectValue) {

            if (estimatedRTT.TryGetValue(serverId, out long rtt)) {
                await NamingServiceLayer.Instance.Write(
                    serverId, 
                    partitionId, 
                    objectId, 
                    objectValue,
                    2 * rtt + MIN_TIMOUT);
            }
        }

        private async void PingAliveServers() {
            while(true) {
                //Ping all servers
                Task[] tasks = estimatedRTT.Keys.Where(id => selfId != id).Select(PingSingleServer).ToArray();
                Task.WaitAll(tasks);
                await Task.Delay(PING_DELAY);
            }
        }

        private async Task PingSingleServer(string serverId) {
            if (estimatedRTT.TryGetValue(serverId, out long rtt)) {
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                await NamingServiceLayer.Instance.Ping(serverId, 2 * rtt + MIN_TIMOUT);
                stopWatch.Stop();
                long timeEllapsed = stopWatch.ElapsedMilliseconds;
                lock(estimatedRTT) {
                    if (estimatedRTT.ContainsKey(serverId)) {
                        // newRTT = sampledRTT * 0.8 + oldRTT * 0.2
                        estimatedRTT[serverId] = (8 * timeEllapsed) / 10 + (2 * rtt) / 10;
                    }
                }
            }
        }

        public void OnReplicaFailure(object sender, IdFailureEventArgs args) {
            string serverId = args.Id;
            estimatedRTT.TryRemove(serverId, out _);
        }
    }
}
