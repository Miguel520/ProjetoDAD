using System.Collections.Generic;
using System.Linq;

namespace PCS.Communications {

    public class CreateServerArguments {
        public string ServerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public int MinDelay { get; set; }
        public int MaxDelay { get; set; }
        public int Version { get; set; }

        //arguments: server_id host port min_delay max_delay version
        public string CollapseArguments() {
            string[] args = { 
                ServerId,
                Host,
                Port.ToString(),
                MinDelay.ToString(),
                MaxDelay.ToString(),
                Version.ToString()    
            };

            return string.Join(" ", args);
        }
    }

    public class CreateClientArguments {
        public string Username { get; set; }
        public string Host { get; set; }
        public int Port{ get; set; }
        public string Script { get; set; }

        public List<string> NameServersUrls { get; set; }

        public int Version { get; set; }

        //arguments: username host port script version (name_server_url)+
        public string CollapseArguments() {
            string[] args = {
                Username,
                Host,
                Port.ToString(),
                Script,
                Version.ToString()
            };

            return string.Join(" ", args.Concat(NameServersUrls));
        }
    }
}
