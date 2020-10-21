﻿using System;
using System.Collections.Generic;
using System.Text;

namespace Client.Commands
{
    public interface ICommandHandler
    {
        void OnReadCommand(ReadCommand command);

        void OnWriteCommand(WriteCommand command);

        void OnListServerCommand(ListServerCommand command);

        void OnListGlobalCommand(ListGlobalCommand command);

        void OnWaitCommand(WaitCommand command);

        void OnBeginRepeatCommand(BeginRepeatCommand command);

        void OnEndRepeatCommand(EndRepeatCommand command);
    }
}
