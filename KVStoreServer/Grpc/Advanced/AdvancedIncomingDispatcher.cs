using Common.Utils;
using KVStoreServer.CausalConsistency;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Base;
using KVStoreServer.Storage.Advanced;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Advanced {
    public class AdvancedIncomingDispatcher : BaseIncomingDispatcher {

        private WriteHandler writeHandler = null;
        private ListServerHandler listServerHandler = null;

        private BroadcastWriteHandler broadcastWriteHandler = null;
        private BroadcastFailureHandler broadcastFailureHandler = null;
        public AdvancedIncomingDispatcher(ServerConfiguration serverConfig) 
            : base(serverConfig) { }

        public void BindWriteHandler(WriteHandler handler) {
            writeHandler = handler;
        }

        public void BindListServerHandler(ListServerHandler handler) {
            listServerHandler = handler;
        }

        public void BindBroadcastWriteHandler(BroadcastWriteHandler handler) {
            broadcastWriteHandler = handler;
        }

        public void BindBroadcastFailureHandler(BroadcastFailureHandler handler) {
            broadcastFailureHandler = handler;
        }

        public async Task<ImmutableVectorClock> OnWrite(WriteArguments arguments) {
            Conditions.AssertState(writeHandler != null);
            WaitFreeze();
            await WaitDelay();
            return writeHandler(arguments);
        }

        public async Task<IEnumerable<StoredObjectDto>> OnListServer() {
            Conditions.AssertState(listServerHandler != null);
            WaitFreeze();
            await WaitDelay();
            return listServerHandler();
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
