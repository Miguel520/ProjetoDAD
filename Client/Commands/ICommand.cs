﻿
namespace Client.Commands
{
    public interface ICommand
    {
        void Accept(ICommandHandler handler);
    }
}
