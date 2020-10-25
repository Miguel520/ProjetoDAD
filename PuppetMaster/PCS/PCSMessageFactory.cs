﻿using Common.Protos.ProcessCreation;

namespace PuppetMaster.PCS {

    /*
     * Factory class to build messages related to PCS communication
     */
    public class PCSMessageFactory {
        private PCSMessageFactory() {
        }

        public static CreateServerRequest BuildCreateServerRequest(
            int serverId,
            string host,
            int port,
            int minDelay,
            int maxDelay) {

            return new CreateServerRequest {
                ServerId = serverId,
                Host = host,
                Port = port,
                MinDelay = minDelay,
                MaxDelay = maxDelay
            };
        }

        public static CreateClientRequest BuildCreateClientRequest(
            string username,
            string host,
            int port,
            string scriptFile,
            string nameServerHost,
            int nameServerPort) {

            return new CreateClientRequest {
                Username = username,
                Host = host,
                Port = port,
                Script = scriptFile,
                ServerHost = nameServerHost,
                ServerPort = nameServerPort
            };
        }
    }
}
