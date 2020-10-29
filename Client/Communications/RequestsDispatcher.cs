using System;
using Client.Configuration;

namespace Client.Communications {
    class RequestsDispatcher {

        private readonly ClientConfiguration clientConfig;

        public RequestsDispatcher(ClientConfiguration configuration) {
            clientConfig = configuration;
        }

        public void Status() {
            Console.WriteLine(
                $"Client with username {clientConfig.Username} " +
                $"is running at {clientConfig.Url} " +
                $"with script called {clientConfig.Script}");
        }
    }
}
