using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using KVStoreServer.Configuration;
using KVStoreServer.Replication;

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
            ServerConfiguration serverConfiguration,
            ReplicationService replicationService,
            PartitionsDB partitionsDB) {

            this.config = serverConfiguration;
            this.replicationService = replicationService;
            this.partitionsDB = partitionsDB;
        }

        public Task<string> Read(ReadArguments args) {
            return null;
        }

        public async Task Write(WriteArguments args) {
            WaitFreeze();
            await WaitDelay();
            replicationService.Write(args);
        }

        public Task<IEnumerable<Tuple<string, bool>>> List() {
            return null;
        }

        public async Task JoinPartition(JoinPartitionArguments args) {
            WaitFreeze();
            await WaitDelay();
            partitionsDB.AddPartition(args.Name, args.Members, args.MasterId);
        }

        public async Task Status() {
            WaitFreeze();
            await WaitDelay();
            Console.WriteLine(
                $"Server with id {config.ServerId} " +
                $"running at {config.Url}");
            Console.WriteLine(
                $"  Partitions: {string.Join(", ", partitionsDB.ListPartitions())}");
            Console.WriteLine($"  Status: {(freezed ? "Freezed" : "Unfreezed")}");
        }

        public void Freeze() {
            lock(freezeLock) {
                freezed = true;
            }
        }

        public void Unfreeze() {
            lock(freezeLock) {
                freezed = false;
                Monitor.PulseAll(this);
            }
        }

        public async Task Lock(LockArguments args) {
            WaitFreeze();
            await WaitDelay();
            replicationService.Lock(args);
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
                    Monitor.Wait(this);
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
