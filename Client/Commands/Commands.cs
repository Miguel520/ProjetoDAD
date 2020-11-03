namespace Client.Commands
{
    public class ReadCommand : ICommand {
        public string PartitionId { get; set; }
        public string ObjectId { get; set; }
        public string ServerId { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnReadCommand(this);
        }
    }

    public class WriteCommand : ICommand {
        public string PartitionId { get; set; }
        public string ObjectId { get; set; }
        public string Value { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnWriteCommand(this);
        }
    }

    public class ListServerCommand : ICommand {
        public string ServerId { get; set; }

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
        public string Time { get; set; }

        public void Accept(ICommandHandler handler) {
            handler.OnWaitCommand(this);
        }
    }

    public class BeginRepeatCommand : ICommand {
        public int Iterations { get; set; }

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
