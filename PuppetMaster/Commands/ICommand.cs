namespace PuppetMaster.Commands {

    /*
     * Base visited class for commands
     */
    public interface ICommand {
        void Accept(ICommandHandler handler);
    }
}
