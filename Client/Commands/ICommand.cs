using System;
using System.Collections.Generic;
using System.Text;

namespace Client.Commands
{
    public interface ICommand
    {
        void Accept(ICommandHandler handler);
    }
}
