namespace PuppetMaster.Commands {

    /*
     * Visitor class for commands
     */
    public interface ICommandHandler {

        void OnReplicationFactorCommand(ReplicationFactorCommand command);

        void OnCreateServerCommand(CreateServerCommand command);

        void OnCreatePartitionCommand(CreatePartitionCommand command);

        void OnCreateClientCommand(CreateClientCommand command);

        void OnStatusCommand(StatusCommand command);

        void OnCrashServerCommand(CrashServerCommand command);

        void OnFreezeServerCommand(FreezeServerCommand command);

        void OnUnfreezeServerCommand(UnfreezeServerCommand command);

        void OnWaitCommand(WaitCommand command);
    }
}
