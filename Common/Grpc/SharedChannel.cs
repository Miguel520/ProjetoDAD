using Grpc.Core;
using Grpc.Net.Client;
using System;

namespace Common.Grpc {

    /*
     * Shared rpc channel
     * Users must never shutdown the channel
     * New users should add a new ref to say they are using the channel
     * When they are done with the channel they must drop their ref
     */
    public sealed class SharedChannel : ChannelBase {

        private readonly GrpcChannel channel;
        private int refs = 1;
        internal SharedChannel(GrpcChannel channel) : base(channel.Target) {
            this.channel = channel;
        }

        internal void AddRef() {
            lock(this) {
                refs++;
            }
        }

        internal void DropRef() {
            lock(this) {
                refs--;
                if (refs == 0) {
                    channel.ShutdownAsync().Wait();
                }
            }
        }

        public override CallInvoker CreateCallInvoker() {
            return channel.CreateCallInvoker();
        }
    }
}
