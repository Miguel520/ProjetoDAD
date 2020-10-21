using Client.Commands;
using System;
using System.Collections.Generic;
using System.Text;

namespace Client
{
    public class ClientController : ICommandHandler
    {

        public ClientController() { }

        public void OnBeginRepeatCommand(BeginRepeatCommand command)
        {
            
        }

        public void OnEndRepeatCommand(EndRepeatCommand command)
        {
            throw new NotImplementedException();
        }

        public void OnListGlobalCommand(ListGlobalCommand command)
        {
            throw new NotImplementedException();
        }

        public void OnListServerCommand(ListServerCommand command)
        {
            throw new NotImplementedException();
        }

        public void OnReadCommand(ReadCommand command)
        {
            throw new NotImplementedException();
        }

        public void OnWaitCommand(WaitCommand command)
        {
            throw new NotImplementedException();
        }

        public void OnWriteCommand(WriteCommand command)
        {
            throw new NotImplementedException();
        }
    }
}
