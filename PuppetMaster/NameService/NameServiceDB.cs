using System.Collections.Generic;
using System.Collections.Immutable;

namespace PuppetMaster.NameService {

    /*
     * Class responsible for the storage of (id, url) pairs
     * in orther to facilitate discovery of servers and clients
     */
    public class NameServiceDB {

        private readonly Dictionary<string, string> servers;
        private readonly Dictionary<string, string> clients;

        public NameServiceDB() {
            servers = new Dictionary<string, string>();
            clients = new Dictionary<string, string>();
        }

        /*
         * Adds a server with the given id and url
         * Returns true if successfull and false otherwise
         * If the id already exists returns false and does NOT replace url
         */
        public bool TryAddServer(string id, string url) {
            return servers.TryAdd(id, url);
        }

        /*
         * Trys to find the server with the given id
         * Returns true if a server with given id exists and writes the value in string.
         * Otherwise, returns false and url is set to null
         */
        public bool TryLookupServer(string id, out string url) {
            return servers.TryGetValue(id, out url);
        }

        public void RemoveServer(string id) {
            servers.Remove(id);
        }

        public ImmutableDictionary<string, string> ServersMapping() {
            lock(servers) {
                return ImmutableDictionary.ToImmutableDictionary(servers);
            }
        }

        public ImmutableList<string> ListServers() {
            lock(servers) {
                return ImmutableList.ToImmutableList(servers.Values);
            }
        }

        /*
         * Adds a client with the given username and url
         * Returns true if successfull and false otherwise
         * If the username already exists returns false and does NOT replace url
         */
        public bool TryAddClient(string username, string url) {
            return clients.TryAdd(username, url);
        }

        /*
         * Trys to find the client with the given username
         * Returns true if a client with given username exists and 
         * writes the value in url. Otherwise, returns false and url is set to null
         */
        public bool TryLookupClient(string username, out string url) {
            return clients.TryGetValue(username, out url);
        }

        public void RemoveClient(string username) {
            clients.Remove(username);
        }

        public ImmutableList<string> ListClients() {
            lock(clients) {
                return ImmutableList.ToImmutableList(clients.Values);
            }
        }
    }
}
