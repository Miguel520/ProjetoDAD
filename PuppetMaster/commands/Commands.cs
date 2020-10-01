﻿/*
 * Definitions of DTO classes for each console command
 * The classes should not hold any logic and should implement
 * the visitor pattern with respect to the class CommandHandler
 */
namespace PuppetMaster.Commands {

    public class ReplicationFactorCommand : ICommand {
        public int ReplicationFactor;

        public void Accept(CommandHandler handler) {
            handler.OnReplicationFactorCommand(this);
        }
    }

    public sealed class CreateServerCommand : ICommand {
        public int ServerId;
        public string URL;
        // Delays for the servers in milliseconds
        public int MinDelay, MaxDelay;

        public void Accept(CommandHandler handler) {
            handler.OnCreateServerCommand(this);
        }
    }

    public sealed class CreatePartitionCommand : ICommand {
        public int NumberOfReplicas;
        public string PartitionName;
        public int[] ServerIds;

        public void Accept(CommandHandler handler) {
            handler.OnCreatePartitionCommand(this);
        }
    }

    public sealed class CreateClientCommand : ICommand {
        public string Username;
        public string URL;
        public string ScriptFile;

        public void Accept(CommandHandler handler) {
            handler.OnCreateClientCommand(this);
        }
    }

    public sealed class StatusCommand : ICommand {

        public void Accept(CommandHandler handler) {
            handler.OnStatusCommand(this);
        }
    }

    public sealed class CrashServerCommand : ICommand {
        public int ServerId;

        public void Accept(CommandHandler handler) {
            handler.OnCrashServerCommand(this);
        }
    }

    public sealed class FreezeServerCommand : ICommand {
        public int ServerId;

        public void Accept(CommandHandler handler) {
            handler.OnFreezeServerCommand(this);
        }
    }

    public sealed class UnfreezeServerCommand : ICommand {
        public int ServerId;

        public void Accept(CommandHandler handler) {
            handler.OnUnfreezeServerCommand(this);
        }
    }

    public sealed class WaitCommand : ICommand {
        // Time for the PuppetMaster to sleep in milliseconds
        public int SleepTime;

        public void Accept(CommandHandler handler) {
            handler.OnWaitCommand(this);
        }
    }
}