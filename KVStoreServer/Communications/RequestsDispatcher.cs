using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Common.Protos.KeyValueStore;
using KVStoreServer.Configuration;
using KVStoreServer.Replication;
using KVStoreServer.Storage;

namespace KVStoreServer.Communications {

    /*
     * Class responsible to dispatch requests them to the apropriate handler, 
     * returnin the a response task when aplicable
     * This class should work with domain classes only
     * Also controlls any delays and freezes for the server requests
     */
    public class RequestsDispatcher {

        private readonly ServerConfiguration config;
        private readonly ReplicationService replicationService;
        private readonly PartitionsDB partitionsDB;

        private readonly Random random = new Random();

        private bool freezed = false;
        private readonly object freezeLock = new object();

        public RequestsDispatcher(
            ServerConfiguration config,
            ReplicationService replicationService,
            PartitionsDB partitionsDB) {

            this.config = config;
            this.replicationService = replicationService;
            this.partitionsDB = partitionsDB;
        }

        public async Task<string> Read(ReadArguments args) {
            WaitFreeze();
            await WaitDelay();
            return replicationService.Read(args);
        }

        public async Task Write(WriteArguments args) {
            WaitFreeze();
            await WaitDelay();
            replicationService.Write(args);
        }

        public async Task<ImmutableList<StoredValueDto>> List() {
            WaitFreeze();
            await WaitDelay();
            replicationService.TryGetAllObjects(out List<StoredValueDto> objects);
            return objects.ToImmutableList();
        }

        public async Task JoinPartition(JoinPartitionArguments args) {
            WaitFreeze();
            await WaitDelay();
            partitionsDB.AddPartition(args.PartitionId, args.Members, args.MasterId);
        }

        public void Status() {
            // Sstatus should not wait for debug purposes
            // WaitFreeze();
            // await WaitDelay();
            Console.WriteLine(
                "[{0}] Server with id {1} running at {2}",
                 DateTime.Now.ToString("HH:mm:ss"),
                 config.ServerId, 
                 config.Url);
            
            Console.WriteLine(
                "[{0}]  Partitions: {1}",
                DateTime.Now.ToString("HH:mm:ss"),
                string.Join(", ", partitionsDB.ListPartitions()));
            
            Console.WriteLine(
                "[{0}]  Status: {1}",
                DateTime.Now.ToString("HH:mm:ss"), 
                freezed ? "Freezed" : "Unfreezed");
        }

        public void Freeze() {
            lock(freezeLock) {
                freezed = true;
            }
            Console.WriteLine("[{0}] Server Freezed", DateTime.Now.ToString("HH:mm:ss"));
        }

        public void Unfreeze() {
            lock(freezeLock) {
                freezed = false;
                Monitor.PulseAll(freezeLock);
            }
            Console.WriteLine("[{0}] Server Unfreezed", DateTime.Now.ToString("HH:mm:ss"));
        }

        public async Task Lock(LockArguments args) {
            WaitFreeze();
            await WaitDelay();
            replicationService.Lock(args);
        }

        public void Crash() {
            Console.WriteLine("[{0}] Server Crashed", DateTime.Now.ToString("HH:mm:ss"));
            Environment.Exit(0);
        }

        public async Task WriteObject(WriteObjectArguments args) {
            WaitFreeze();
            await WaitDelay();
            replicationService.WriteObject(args);
        }

        private void WaitFreeze() {
            lock(freezeLock) {
                // If the server is frozen then all threads must wait
                // while it is unfrozen and the requests are no longer
                // processed
                while (freezed) {
                    Monitor.Wait(freezeLock);
                }
            }
        }

        private async Task WaitDelay() {
            if (config.MinDelay != 0 || config.MaxDelay != 0) {

                int delay = random.Next(
                    config.MinDelay,
                    config.MaxDelay);
                await Task.Delay(delay);
            }
        }
    }
}
