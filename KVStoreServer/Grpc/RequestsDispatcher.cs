using Common.Protos.KeyValueStore;
using Common.Protos.ServerConfiguration;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using KVStoreServer.Replication;

namespace KVStoreServer.Grpc {

    /*
     * Class responsible to dispatch requests them to the apropriate handler, 
     * returnin the a response task when aplicable
     * This class should work with domain classes only
     * Also controlls any delays and freezes for the server requests
     */
    public class RequestsDispatcher {

        private readonly PartitionsDB partitionsDB;

        public RequestsDispatcher(PartitionsDB partitionsDB) {
            this.partitionsDB = partitionsDB;
        }

        // FIXME: Domain params
        public Task<ReadResponse> Read(ReadRequest request) {
            return null;
        }

        // FIXME: Domain params
        public Task<WriteResponse> Write(WriteRequest request) {
            return null;
        }

        // FIXME: Domain params
        public Task<ListResponse> List(ListRequest request) {
            return null;
        }

        public void JoinPartition(string name, IEnumerable<Tuple<int, string>> members) {
            partitionsDB.AddPartition(name, members);
        }

        // FIXME: Domain params
        public Task<StatusResponse> Status(StatusRequest request) {
            return null;
        }

        public void Freeze() {
            // TODO: Implement
        }

        public void Unfreeze() {
            // TODO: Implement
        }
    }
}
