using System.Linq;

namespace Client.Commands
{
    public class CommandParser
    {
        private const string READ_COMMAND = "read";
        private const string WRITE_COMMAND = "write";
        private const string LIST_SERVER_COMMAND = "listServer";
        private const string LIST_GLOBAL_COMMAND = "listGlobal";
        private const string WAIT_COMMAND = "wait";
        private const string BEGIN_REPEAT_COMMAND = "begin-repeat";
        private const string END_REPEAT_COMMAND = "end-repeat";

        public static bool TryParse(string line, out ICommand command) {
            command = null;

            string[] inputTokens = line.Split(" ");

            string inputCommand = inputTokens[0];
            string[] inputArguments = inputTokens.Skip(1).ToArray();

            switch (inputCommand) {
                case READ_COMMAND:
                    return TryParseReadCommand(inputArguments, out command);
                case WRITE_COMMAND:
                    return TryParseWriteCommand(inputArguments, out command);
                case LIST_SERVER_COMMAND:
                    return TryParseListServerCommand(inputArguments, out command);
                case LIST_GLOBAL_COMMAND:
                    return TryParseListGlobalCommand(inputArguments, out command);
                case WAIT_COMMAND:
                    return TryParseWaitCommand(inputArguments, out command);
                case BEGIN_REPEAT_COMMAND:
                    return TryParseBeginRepeatCommand(inputArguments, out command);
                case END_REPEAT_COMMAND:
                    return TryParseEndRepeatCommand(inputArguments, out command);
                default:
                    return false;
            }
        }

        private static bool TryParseReadCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 3) return false;

            string partitionId = arguments[0];
            if (!int.TryParse(arguments[1], out int objectId)) return false;
            if (!int.TryParse(arguments[2], out int serverId)) return false;

            command = new ReadCommand {
                partitionId = partitionId,
                objectId = objectId,
                serverId = serverId
            };

            return true;
        }

        private static bool TryParseWriteCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 3) return false;

            string partitionId = arguments[0];
            if (!int.TryParse(arguments[1], out int objectId)) return false;
            string value = arguments[2];

            command = new WriteCommand {
                partitionId = partitionId,
                objectId = objectId,
                value = value
            };

            return true;
        }

        private static bool TryParseListServerCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int serverId)) return false;

            command = new ListServerCommand {
                serverId = serverId
            };

            return true;
        }

        private static bool TryParseListGlobalCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 0) return false;

            command = new ListGlobalCommand { };

            return true;
        }

        private static bool TryParseWaitCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int x)) return false;

            command = new WaitCommand {
                x = x
            };

            return true;
        }

        public static bool TryParseBeginRepeatCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int x)) return false;

            command = new BeginRepeatCommand {
                x = x
            };

            return true;
        }

        public static bool TryParseEndRepeatCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 0) return false;

            command = new EndRepeatCommand { };

            return true;
        }
    }
}
