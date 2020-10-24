/*
 * Definitions of DTO classes for each console command
 * The classes should not hold any logic and should implement
 * the visitor pattern with respect to the class CommandHandler
 */
namespace PuppetMaster.Commands {

    public class ReplicationFactorCommand : ICommand {
        public int ReplicationFactor;

        public void Accept(ICommandHandler handler) {
            handler.OnReplicationFactorCommand(this);
        }
    }

    public sealed class CreateServerCommand : ICommand {
        public int ServerId;
        public string Host;
        public int Port;
        // Delays for the servers in milliseconds
        public int MinDelay, MaxDelay;

        public void Accept(ICommandHandler handler) {
            handler.OnCreateServerCommand(this);
        }
    }

    public sealed class CreatePartitionCommand : ICommand {
        public int NumberOfReplicas;
        public string PartitionName;
        public int[] ServerIds;

        public void Accept(ICommandHandler handler) {
            handler.OnCreatePartitionCommand(this);
        }
    }

    public sealed class CreateClientCommand : ICommand {
        public string Username;
        public string Host;
        public int Port;
        public string ScriptFile;

        public void Accept(ICommandHandler handler) {
            handler.OnCreateClientCommand(this);
        }
    }

    public sealed class StatusCommand : ICommand {

        public void Accept(ICommandHandler handler) {
            handler.OnStatusCommand(this);
        }
    }

    public sealed class CrashServerCommand : ICommand {
        public int ServerId;

        public void Accept(ICommandHandler handler) {
            handler.OnCrashServerCommand(this);
        }
    }

    public sealed class FreezeServerCommand : ICommand {
        public int ServerId;

        public void Accept(ICommandHandler handler) {
            handler.OnFreezeServerCommand(this);
        }
    }

    public sealed class UnfreezeServerCommand : ICommand {
        public int ServerId;

        public void Accept(ICommandHandler handler) {
            handler.OnUnfreezeServerCommand(this);
        }
    }

    public sealed class WaitCommand : ICommand {
        // Time for the PuppetMaster to sleep in milliseconds
        public int SleepTime;

        public void Accept(ICommandHandler handler) {
            handler.OnWaitCommand(this);
        }
    }
}