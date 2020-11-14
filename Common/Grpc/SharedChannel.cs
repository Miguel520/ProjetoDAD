using Grpc.Net.Client;

namespace Common.Grpc {

    /*
     * Shared rpc channel
     * Users must never shutdown the channel
     * When last user drops the reference the channel is shutdown
     */
    class SharedChannel {
        public SharedChannel(GrpcChannel channel) {
            Channel = channel;
        }

        ~SharedChannel() {
            Channel.ShutdownAsync().Wait();
        }

        public GrpcChannel Channel { get; }
    }
}
