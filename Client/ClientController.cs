using Client.Commands;
using System.Collections.Generic;
using System.Threading;

namespace Client {
    public class ClientController : ICommandHandler {

        private bool insideLoop;
        private readonly List<ICommand> loopCommands = new List<ICommand>();
        private int numReps = 0;

        public ClientController() { }

        public void OnBeginRepeatCommand(BeginRepeatCommand command) {
            insideLoop = true;
            numReps = command.x;
            loopCommands.Clear();
        }

        public void OnEndRepeatCommand(EndRepeatCommand command) {
            for (int i = 0; i < numReps; i++) {
                loopCommands.ForEach(command => command.Accept(this));
            }
            insideLoop = false;
            numReps = 0;
        }

        public void OnListGlobalCommand(ListGlobalCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
        }

        public void OnListServerCommand(ListServerCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
        }

        public void OnReadCommand(ReadCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
        }

        public void OnWaitCommand(WaitCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
            Thread.Sleep(command.x);
        }

        public void OnWriteCommand(WriteCommand command) {
            if (insideLoop) {
                loopCommands.Add(command);
                return;
            }
        }
    }
}
