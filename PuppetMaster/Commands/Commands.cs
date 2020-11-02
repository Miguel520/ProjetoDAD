/*
 * Definitions of DTO classes for each console command
 * The classes should not hold any logic and should implement
 * the visitor pattern with respect to the class CommandHandler
 */
namespace PuppetMaster.Commands {

    public class ReplicationFactorCommand : ICommand {
        public int ReplicationFactor { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnReplicationFactorCommand(this);
        }
    }

    public sealed class CreateServerCommand : ICommand {
        public string ServerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        // Delays for the servers in milliseconds
        public int MinDelay { get; set; }
        public int MaxDelay { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnCreateServerCommand(this);
        }
    }

    public sealed class CreatePartitionCommand : ICommand {
        public int NumberOfReplicas { get; set; }
        public string PartitionId { get; set; }
        public string[] ServerIds { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnCreatePartitionCommand(this);
        }
    }

    public sealed class CreateClientCommand : ICommand {
        public string Username { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public string ScriptFile { get; set; }

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
        public string ServerId { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnCrashServerCommand(this);
        }
    }

    public sealed class FreezeServerCommand : ICommand {
        public string ServerId { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnFreezeServerCommand(this);
        }
    }

    public sealed class UnfreezeServerCommand : ICommand {
        public string ServerId { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnUnfreezeServerCommand(this);
        }
    }

    public sealed class WaitCommand : ICommand {
        // Time for the PuppetMaster to sleep in milliseconds
        public int SleepTime { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnWaitCommand(this);
        }
    }
}