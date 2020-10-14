using System;

using PuppetMaster.Commands;

namespace PuppetMaster {

    /*
     * Class responsible for the general control of the Puppet Master state
     * Processes received commands and applies necessary operations
     */
    public class PMController : ICommandHandler {
        public PMController() {
        }

        public void OnReplicationFactorCommand(ReplicationFactorCommand command) {
            throw new NotImplementedException();
        }

        public void OnCreateServerCommand(CreateServerCommand command) {
            throw new NotImplementedException();
        }

        public void OnCreateClientCommand(CreateClientCommand command) {
            throw new NotImplementedException();
        }

        public void OnCreatePartitionCommand(CreatePartitionCommand command) {
            throw new NotImplementedException();
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
