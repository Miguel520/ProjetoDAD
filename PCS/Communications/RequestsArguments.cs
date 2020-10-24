using Common.Protos.ProcessCreation;

namespace PCS.Communications {

    public class CreateServerArguments {
        public int ServerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public int MinDelay { get; set; }
        public int MaxDelay { get; set; }

        //arguments: server_id host port min_delay max_delay
        public string CollapseArguments() {
            string[] args = { 
                ServerId.ToString(),
                Host,
                Port.ToString(),
                MinDelay.ToString(),
                MaxDelay.ToString()};

            return string.Join(" ", args);
        }
    }

    public class CreateClientArguments {
        public string Username { get; set; }
        public string Host { get; set; }
        public int Port{ get; set; }
        public string Script { get; set; }

        //TODO not implemented in client yet
        //arguments: username host port script_path
        public string CollapseArguments() {
            string[] args = { 
                Username,
                Host,
                Port.ToString(),
                Script};

            return string.Join(" ", args);
        }
    }
}
