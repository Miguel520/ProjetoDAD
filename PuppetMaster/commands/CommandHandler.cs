namespace PuppetMaster.Commands {

    /*
     * Visitor class for commands
     */
    public class CommandHandler {

        public void OnReplicationFactorCommand(ReplicationFactorCommand command) { }

        public void OnCreateServerCommand(CreateServerCommand command) { }

        public void OnCreatePartitionCommand(CreatePartitionCommand command) { }

        public void OnCreateClientCommand(CreateClientCommand command) { }

        public void OnStatusCommand(StatusCommand command) { }

        public void OnCrashServerCommand(CrashServerCommand command) { }

        public void OnFreezeServerCommand(FreezeServerCommand command) { }

        public void OnUnfreezeServerCommand(UnfreezeServerCommand command) { }

        public void OnWaitCommand(WaitCommand command) { }
    }
}
