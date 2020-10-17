using Common.Utils;
using System;

using PuppetMaster.Commands;
using PuppetMaster.PCS;

namespace PuppetMaster {

    /*
     * Class responsible for the general control of the Puppet Master state
     * Processes received commands and applies necessary operations
     */
    public class PMController : ICommandHandler {

        private int replicationFactor = 0;
        private readonly NameService nameService = new NameService();

        public PMController() {
        }

        public void OnReplicationFactorCommand(ReplicationFactorCommand command) {
            if (replicationFactor != 0) {
                Console.Error.WriteLine("Replication Factor already set to {0}", replicationFactor);
                return;
            }
            replicationFactor = command.ReplicationFactor;
        }

        public void OnCreateServerCommand(CreateServerCommand command) {
            int serverId = command.ServerId;
            string url = command.URL;

            // URL should be in the form http://<PCS host>:<Server Port>
            if (!HttpURLs.TryParseHost(url, out string PCSHost)) {
                Console.Error.WriteLine("Invalid Host in URL '{0}'", url);
                return;
            }

            // Check if server already exists
            if (!nameService.TryAddServer(serverId, url)) {
                Console.Error.WriteLine("Server with id {0} already exists", serverId);
                return;
            }

            PCSConnection connection = new PCSConnection(PCSHost);
            if (!connection.CreateServer(serverId, command.MinDelay, command.MaxDelay)) {
                // Remove inserted id if operation failed
                nameService.RemoveServer(serverId);
            }
        }

        public void OnCreatePartitionCommand(CreatePartitionCommand command) {
            throw new NotImplementedException();
        }

        public void OnCreateClientCommand(CreateClientCommand command) {
            string username = command.Username;
            string url = command.URL;

            // URL should be in the form http://<PCS host>:<Client Port>
            if (!HttpURLs.TryParseHost(url, out string PCSHost)) {
                Console.Error.WriteLine("Invalid Host in URL '{0}'", url);
                return;
            }

            // Check if client already exists
            if (!nameService.TryAddClient(username, url)) {
                Console.Error.WriteLine("Client with username {0} already exists", username);
                return;
            }

            PCSConnection connection = new PCSConnection(PCSHost);
            if (!connection.CreateClient(username, command.ScriptFile)) {
                // Remove inserted username if operation failed
                nameService.RemoveClient(username);
            }
        }

        public void OnStatusCommand(StatusCommand command) {
            throw new NotImplementedException();
        }

        public void OnCrashServerCommand(CrashServerCommand command) {
            throw new NotImplementedException();
        }

        public void OnFreezeServerCommand(FreezeServerCommand command) {
            throw new NotImplementedException();
        }

        public void OnUnfreezeServerCommand(UnfreezeServerCommand command) {
            throw new NotImplementedException();
        }

        public void OnWaitCommand(WaitCommand command) {
            throw new NotImplementedException();
        }
    }
}
