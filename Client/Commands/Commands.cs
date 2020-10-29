namespace Client.Commands
{
    public class ReadCommand : ICommand {
        public string partitionId;
        public int objectId;
        public int serverId;

        public void Accept(ICommandHandler handler) {
            handler.OnReadCommand(this);
        }
    }

    public class WriteCommand : ICommand {
        public string partitionId;
        public int objectId;
        public string value;

        public void Accept(ICommandHandler handler) {
            handler.OnWriteCommand(this);
        }
    }

    public class ListServerCommand : ICommand {
        public int serverId;

        public void Accept(ICommandHandler handler) {
            handler.OnListServerCommand(this);
        }
    }

    public class ListGlobalCommand : ICommand {
        public void Accept(ICommandHandler handler) {
            handler.OnListGlobalCommand(this);
        }
    }

    public class WaitCommand : ICommand {
        public int x;

        public void Accept(ICommandHandler handler) {
            handler.OnWaitCommand(this);
        }
    }

    public class BeginRepeatCommand : ICommand {
        public int x;

        public void Accept(ICommandHandler handler) {
            handler.OnBeginRepeatCommand(this);
        }
    }

    public class EndRepeatCommand : ICommand {
        public void Accept(ICommandHandler handler) {
            handler.OnEndRepeatCommand(this);
        }
    }
}
