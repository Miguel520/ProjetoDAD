using Common.CausalConsistency;
using Common.Utils;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Base;
using KVStoreServer.Replication.Advanced;
using KVStoreServer.Storage.Advanced;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Advanced {
    public class AdvancedIncomingDispatcher : BaseIncomingDispatcher {

        private ReadHandler readHandler = null;
        private WriteHandler writeHandler = null;
        private ListServerHandler listServerHandler = null;

        private BroadcastWriteDeliveryHandler broadcastWriteHandler = null;
        private BroadcastFailureDeliveryHandler broadcastFailureHandler = null;
        public AdvancedIncomingDispatcher(ServerConfiguration serverConfig) 
            : base(serverConfig) { }

        public void BindReadHandler(ReadHandler handler) {
            readHandler = handler;
        }

        public void BindWriteHandler(WriteHandler handler) {
            writeHandler = handler;
        }

        public void BindListServerHandler(ListServerHandler handler) {
            listServerHandler = handler;
        }

        public void BindBroadcastWriteDeliveryHandler(BroadcastWriteDeliveryHandler handler) {
            broadcastWriteHandler = handler;
        }

        public void BindBroadcastFailureDeliveryHandler(BroadcastFailureDeliveryHandler handler) {
            broadcastFailureHandler = handler;
        }

        public async Task<(string, ImmutableVectorClock)> OnRead(ReadArguments arguments) {
            Conditions.AssertState(readHandler != null);
            WaitFreeze();
            await WaitDelay();
            return readHandler(arguments);
        }

        public async Task<ImmutableVectorClock> OnWrite(WriteArguments arguments) {
            Conditions.AssertState(writeHandler != null);
            WaitFreeze();
            await WaitDelay();
            return writeHandler(arguments);
        }

        public async Task<(IEnumerable<StoredObjectDto>, IEnumerable<PartitionTimestampDto>)> OnListServer() {
            Conditions.AssertState(listServerHandler != null);
            //WaitFreeze();
            //await WaitDelay();
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
