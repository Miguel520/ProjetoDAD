using Common.Utils;
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
        private const string BEGIN_TIMER_COMMAND = "begin-timer";
        private const string END_TIMER_COMMAND = "end-timer";

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
                case BEGIN_TIMER_COMMAND:
                    return TryParseBeginTimerCommand(inputArguments, out command);
                case END_TIMER_COMMAND:
                    return TryParseEndTimerCommand(inputArguments, out command);
                default:
                    return false;
            }
        }

        private static bool TryParseReadCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 2 && arguments.Length != 3) return false;

            string partitionId = arguments[0];
            string objectId = arguments[1];
            string serverId = (arguments.Length == 3 ? arguments[2] : null);

            command = new ReadCommand {
                PartitionId = partitionId,
                ObjectId = objectId,
                ServerId = serverId
            };

            return true;
        }

        /*
         * Parse the write command
         * Object values must be one word or be inside ""
         */
        private static bool TryParseWriteCommand(string[] arguments, out ICommand command) {
            command = null;

            // At least 3 arguments are required
            if (arguments.Length < 3) return false;

            string partitionId = arguments[0];
            string objectId = arguments[1];

            string[] valueTokens = Arrays.Slice(arguments, 2, arguments.Length); 
            string value = string.Join(' ', valueTokens);

            // Remove leading and trailing " if necessary
            if (value.StartsWith('"') && value.EndsWith('"')) {
                value = value[1..^1];
            }

            command = new WriteCommand {
                PartitionId = partitionId,
                ObjectId = objectId,
                Value = value
            };

            return true;
        }

        private static bool TryParseListServerCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 1) return false;

            string serverId = arguments[0];

            command = new ListServerCommand {
                ServerId = serverId
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

            string time = arguments[0];

            command = new WaitCommand {
                Time = time
            };

            return true;
        }

        private static bool TryParseBeginRepeatCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 1) return false;

            if (!int.TryParse(arguments[0], out int iterations)) return false;

            command = new BeginRepeatCommand {
                Iterations = iterations
            };

            return true;
        }

        private static bool TryParseEndRepeatCommand(string[] arguments, out ICommand command) {
            command = null;

            if (arguments.Length != 0) return false;

            command = new EndRepeatCommand { };

            return true;
        }

        private static bool TryParseBeginTimerCommand(string[] arguments, out ICommand command) {
            command = default;

            if (arguments.Length != 0) return false;

            command = new BeginTimerCommand { };

            return true;
        }

        private static bool TryParseEndTimerCommand(string[] arguments, out ICommand command) {
            command = default;

            if (arguments.Length != 0) return false;

            command = new EndTimerCommand { };

            return true;
        }
    }
}
