using Common.Utils;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Base;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Advanced {
    public class AdvancedIncomingDispatcher : BaseIncomingDispatcher {

        private BroadcastWriteHandler broadcastWriteHandler = null;
        private BroadcastFailureHandler broadcastFailureHandler = null;
        public AdvancedIncomingDispatcher(ServerConfiguration serverConfig) 
            : base(serverConfig) { }

        public void BindBroadcastWriteHandler(BroadcastWriteHandler handler) {
            broadcastWriteHandler = handler;
        }

        public void BindBroadcastFailureHandler(BroadcastFailureHandler handler) {
            broadcastFailureHandler = handler;
        }

        public async Task OnBroadcastWrite(BroadcastWriteArguments arguments) {
            Conditions.AssertState(broadcastWriteHandler != null);
            WaitFreeze();
            await WaitDelay();
            broadcastWriteHandler(arguments);
        }

        public async Task OnBroadcastFailure(BroadcastFailureArguments arguments) {
            Conditions.AssertArgument(broadcastFailureHandler != null);
            WaitFreeze();
            await WaitDelay();
            broadcastFailureHandler(arguments);
        }
    }
}
