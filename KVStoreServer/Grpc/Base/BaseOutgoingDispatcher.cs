using Grpc.Core;
using System;

namespace KVStoreServer.Grpc.Base {
    public class BaseOutgoingDispatcher {

        private UrlFailureHandler failureHandler = null;

        protected static readonly long DEFAULT_TIMEOUT = 30000;

        public void BindFailureHandler(UrlFailureHandler handler) {
            failureHandler = handler;
        }

        protected void HandleRpcException(string serverUrl, RpcException exception) {
            if (exception.StatusCode == StatusCode.DeadlineExceeded ||
                exception.StatusCode == StatusCode.Internal) {

                failureHandler?.Invoke(serverUrl);
                Console.WriteLine(
                    "[{0}] Replica {1} unavailable",
                    DateTime.Now.ToString("HH:mm:ss"),
                    serverUrl);
            }
            else {
                Console.WriteLine(
                    "[{0}] Error {1} with operation on server {2}",
                    DateTime.Now.ToString("HH:mm:ss"),
                    exception.StatusCode,
                    serverUrl);
            }
        }
    }
}
