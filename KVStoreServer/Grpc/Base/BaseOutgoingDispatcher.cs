using Grpc.Core;
using System;

namespace KVStoreServer.Grpc.Base {
    public class BaseOutgoingDispatcher {

        private UrlFailureDetectionHandler failureHandler = null;

        protected static readonly long DEFAULT_TIMEOUT = 10000;

        public void BindFailureDetectionHandler(UrlFailureDetectionHandler handler) {
            failureHandler = handler;
        }

        protected void HandleRpcException(string serverUrl, RpcException exception) {
            if (exception.StatusCode == StatusCode.DeadlineExceeded ||
                exception.StatusCode == StatusCode.Internal) {

                Console.WriteLine(
                    "[{0}] Replica {1} unavailable",
                    DateTime.Now.ToString("HH:mm:ss"),
                    serverUrl);
                failureHandler?.Invoke(serverUrl);
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
