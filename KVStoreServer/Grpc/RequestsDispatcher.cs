using System.Threading.Tasks;
using Common.Protos.KeyValueStore;
using Common.Protos.ServerConfiguration;

namespace KVStoreServer.Grpc {

    /*
     * Class responsible to transform requests into domain classes
     * and dispatch them to the apropriate handler, returning
     * the response task
     * Also controlls any delays and freezes for the server requests
     */
    public class RequestsDispatcher {

        public RequestsDispatcher() {
        }

        public Task<ReadResponse> Read(ReadRequest request) {
            return null;
        }

        public Task<WriteResponse> Write(WriteRequest request) {
            return null;
        }

        public Task<ListResponse> List(ListRequest request) {
            return null;
        }

        public Task<JoinPartitionResponse> JoinPartition(JoinPartitionRequest request) {
            return null;
        }

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
