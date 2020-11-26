using Common.Utils;
using KVStoreServer.Configuration;
using KVStoreServer.Grpc.Base;
using KVStoreServer.Storage;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KVStoreServer.Grpc.Simple {
    public class SimpleIncomingDispatcher : BaseIncomingDispatcher {

        private ReadHandler readHandler = null;
        private WriteHandler writeHandler = null;
        private ListServerHandler listServerHandler = null;

        private LockHandler lockHandler = null;
        private WriteObjectHandler writeObjectHandler = null;

        public SimpleIncomingDispatcher(ServerConfiguration serverConfig)
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

        public void BindLockHandler(LockHandler handler) {
            lockHandler = handler;
        }

        public void BindWriteObjectHandler(WriteObjectHandler handler) {
            writeObjectHandler = handler;
        }

        public async Task<string> OnRead(ReadArguments arguments) {
            Conditions.AssertState(readHandler != null);
            WaitFreeze();
            await WaitDelay();
            return readHandler(arguments);
        }

        public async Task OnWrite(WriteArguments arguments) {
            Conditions.AssertState(writeHandler != null);
            WaitFreeze();
            await WaitDelay();
            writeHandler(arguments);
        }

        public async Task<IEnumerable<StoredValueDto>> OnListServer() {
            Conditions.AssertState(listServerHandler != null);
            WaitFreeze();
            await WaitDelay();
            return listServerHandler();
        }

        public async Task OnLock(LockArguments arguments) {
            Conditions.AssertState(lockHandler != null);
            WaitFreeze();
            await WaitDelay();
            lockHandler(arguments);
        }

        public async Task OnWriteObject(WriteObjectArguments arguments) {
            Conditions.AssertState(writeObjectHandler != null);
            WaitFreeze();
            await WaitDelay();
            writeObjectHandler(arguments);
        }

        public async Task OnPing() {
            WaitFreeze();
            await WaitDelay();
        }
    }
}
