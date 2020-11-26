using Common.Utils;
using KVStoreServer.Configuration;
using KVStoreServer.Replication;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Base {
    public class BaseIncomingDispatcher {

        private readonly ServerConfiguration config;
        private readonly Random random = new Random();

        private bool freezed = false;
        private readonly object freezeLock = new object();

        private LookupHandler lookupHandler = null;
        private LookupMasterHandler lookupMasterHandler = null;
        private ListPartitionsHandler listPartitionHandler = null;

        private JoinPartitionHandler joinPartitionHandler = null;
        private StatusHandler statusHandler = null;

        public BaseIncomingDispatcher(ServerConfiguration serverConfig) {
            config = serverConfig;
        }

        public void BindLookupHandler(LookupHandler handler) {
            lookupHandler = handler;
        }

        public void BindLookupMasterHandler(LookupMasterHandler handler) {
            lookupMasterHandler = handler;
        }

        public void BindListPartitionHandler(ListPartitionsHandler handler) {
            listPartitionHandler = handler;
        }

        public void BindJoinPartition(JoinPartitionHandler handler) {
            joinPartitionHandler = handler;
        }

        public void BindStatusHandler(StatusHandler handler) {
            statusHandler = handler;
        }

        public async Task<string> OnLookup(string serverId) {
            Conditions.AssertArgument(lookupHandler != null);
            WaitFreeze();
            await WaitDelay();
            if (lookupHandler(serverId, out string serverUrl)) {
                return serverUrl;
            }
            else {
                return null;
            }
        }

        public async Task<string> OnLookupMaster(string partitionId) {
            Conditions.AssertArgument(lookupMasterHandler != null);
            WaitFreeze();
            await WaitDelay();
            if (lookupMasterHandler(partitionId, out string masterUrl)) {
                return masterUrl;
            }
            else {
                return null;
            }
        }

        public async Task<IEnumerable<PartitionServersDto>> OnListPartitions() {
            Conditions.AssertArgument(listPartitionHandler != null);
            WaitFreeze();
            await WaitDelay();
            return listPartitionHandler();
        }

        public async Task OnJoinPartition(JoinPartitionArguments arguments) {
            Conditions.AssertState(joinPartitionHandler != null);
            WaitFreeze();
            await WaitDelay();
            joinPartitionHandler(arguments);
        }

        public void OnStatus() {
            Conditions.AssertState(statusHandler != null);
            statusHandler();
            // Add freeze status
            lock (freezeLock) {
                Console.WriteLine(
                    "[{0}]  Status: {1}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    freezed ? "Freezed" : "Unfreezed");
            }
        }

        public void OnCrash() {
            Console.WriteLine("[{0}] Server Crashed", DateTime.Now.ToString("HH:mm:ss"));
            Environment.Exit(0);
        }

        public void OnFreeze() {
            lock (freezeLock) {
                freezed = true;
            }
            Console.WriteLine("[{0}] Server Freezed", DateTime.Now.ToString("HH:mm:ss"));
        }

        public void OnUnfreeze() {
            lock (freezeLock) {
                freezed = false;
                Monitor.PulseAll(freezeLock);
            }
            Console.WriteLine("[{0}] Server Unfreezed", DateTime.Now.ToString("HH:mm:ss"));
        }

        protected void WaitFreeze() {
            lock (freezeLock) {
                // If the server is frozen then all threads must wait
                // while it is unfrozen and the requests are no longer
                // processed
                while (freezed) {
                    Monitor.Wait(freezeLock);
                }
            }
        }

        protected async Task WaitDelay() {
            if (config.MinDelay != 0 || config.MaxDelay != 0) {

                int delay = random.Next(
                    config.MinDelay,
                    config.MaxDelay);
                await Task.Delay(delay);
            }
        }
    }
}
