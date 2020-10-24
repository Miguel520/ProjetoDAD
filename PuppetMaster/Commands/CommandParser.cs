using Common.Utils;
using System;
using System.Text.RegularExpressions;

namespace PuppetMaster.Commands {
    public class CommandParser {

        private static readonly Regex WHITESPACE_REGEX = new Regex(@"\s+");

        private const string REPLICATION_FACTOR_COMMAND = "ReplicationFactor";
        private const string CREATE_SERVER_COMMAND = "Server";
        private const string CREATE_PARTITION_COMMAND = "Partition";
        private const string CREATE_CLIENT_COMMAND = "Client";
        private const string STATUS_COMMAND = "Status";
        private const string CRASH_COMMAND = "Crash";
        private const string FREEZE_COMMAND = "Freeze";
        private const string UNFREEZE_COMMAND = "Unfreeze";
        private const string WAIT_COMMAND = "Wait";

        /*
         * Function to parse a Puppet Master coomand from a given line
         * :param line: line to parse
         * :out param command line: command to build, will be set to null
         *                          if the line is empty or an error has occured
         * :return: true if a command was successfully created and false
         *          if the line was empty or an error has occurred
         */ 
        public static bool TryParse(string line, out ICommand command) {
            command = null;
            // Clean input string of whitespace
            line = WHITESPACE_REGEX.Replace(line.Trim(), " ");
            // RemoveEmptyEntries to remove empty input
            string[] inputTokens = line.Split(" ", StringSplitOptions.RemoveEmptyEntries);
            // Handle empty line
            if (Arrays.IsEmpty(inputTokens)) {
                return false;
            }

            string inputCommand = inputTokens[0];
            string[] inputArguments = Arrays.Slice(inputTokens, 1, inputTokens.Length);

            switch(inputCommand) {
                case REPLICATION_FACTOR_COMMAND:
                    return TryParseReplicationFactorCommand(inputArguments, out command);
                case CREATE_SERVER_COMMAND:
                    return TryParseCreateServerCommand(inputArguments, out command);
                case CREATE_PARTITION_COMMAND:
                    return TryParseCreatePartitionCommand(inputArguments, out command);
                case CREATE_CLIENT_COMMAND:
                    return TryParseCreateClientCommand(inputArguments, out command);
                case STATUS_COMMAND:
                    return TryParseStatusCommand(inputArguments, out command);
                case CRASH_COMMAND:
                    return TryParseCrashServerCommand(inputArguments, out command);
                case FREEZE_COMMAND:
                    return TryParseFreezeServerCommand(inputArguments, out command);
                case UNFREEZE_COMMAND:
                    return TryParseUnfreezeServerCommand(inputArguments, out command);
                case WAIT_COMMAND:
                    return TryParseWaitCommand(inputArguments, out command);
                default:
                    return false;
            }
        }

        private static bool TryParseReplicationFactorCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int numReplicas)) return false;

            command = new ReplicationFactorCommand { ReplicationFactor = numReplicas };
            return true;
        }

        private static bool TryParseCreateServerCommand(string[] arguments, out ICommand command) {
            command = null;
            if (arguments.Length != 4) return false;

            if (!int.TryParse(arguments[0], out int serverId)) return false;

            string URL = arguments[1];

            if (!int.TryParse(arguments[2], out int minDelay)) return false;

            if (!int.TryParse(arguments[3], out int maxDelay)) return false;

            if (!HttpURLs.TryParseHostAndPort(URL, out string host, out int port)) return false;

            command = new CreateServerCommand {
                ServerId = serverId,
                Host = host,
                Port = port,
                MinDelay = minDelay,
                MaxDelay = maxDelay
            };
            return true;
        }

        private static bool TryParseCreatePartitionCommand(string[] arguments, out ICommand command) {
            command = null;
            if (arguments.Length < 3) return false;

            if (!int.TryParse(arguments[0], out int numberOfReplicas)) return false;

            string partitionName = arguments[1];

            int[] serverIds = new int[arguments.Length - 2];

            for (int i = 2; i < arguments.Length; i++) {
                if (!int.TryParse(arguments[i], out serverIds[i - 2])) return false;
            }

            command = new CreatePartitionCommand {
                NumberOfReplicas = numberOfReplicas,
                PartitionName = partitionName,
                ServerIds = serverIds
            };
            return true;
        }

        private static bool TryParseCreateClientCommand(string[] arguments, out ICommand command) {
            command = null;
            if (arguments.Length != 3) return false;

            string username = arguments[0];
            string URL = arguments[1];

            string scriptFile = arguments[2];

            if (!HttpURLs.TryParseHostAndPort(URL, out string host, out int port)) return false;

            command = new CreateClientCommand {
                Username = username,
                Host = host,
                Port = port,
                ScriptFile = scriptFile
            };
            return true;
        }

        private static bool TryParseStatusCommand(string[] arguments, out ICommand command) {
            command = null;
            if (arguments.Length != 0) return false;
            command = new StatusCommand();
            return true;
        }

        private static bool TryParseCrashServerCommand(string[] arguments, out ICommand command) {
            command = null;
            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int serverId)) return false;

            command = new CrashServerCommand { ServerId = serverId };
            return true;
        }

        private static bool TryParseFreezeServerCommand(string[] arguments, out ICommand command) {
            command = null;
            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int serverId)) return false;

            command = new FreezeServerCommand { ServerId = serverId };
            return true;
        }

        private static bool TryParseUnfreezeServerCommand(string[] arguments, out ICommand command) {
            command = null;
            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int serverId)) return false;

            command = new UnfreezeServerCommand { ServerId = serverId };
            return true;
        }

        private static bool TryParseWaitCommand(string[] arguments, out ICommand command) {
            command = null;
            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int sleepTime)) return false;

            command = new WaitCommand { SleepTime = sleepTime };
            return true;
        }
    }
}
