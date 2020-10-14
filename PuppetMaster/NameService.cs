using System;
using System.Collections.Generic;

namespace PuppetMaster {

    /*
     * Class responsible for the storage of (id, url) pairs
     * in orther to facilitate discovery of servers and clients
     */
    public class NameService {

        private readonly Dictionary<int, string> servers;
        private readonly Dictionary<int, string> clients;

        public NameService() {
            servers = new Dictionary<int, string>();
            clients = new Dictionary<int, string>();
        }

        /*
         * Adds a server with the given id and url
         * Returns true if successfull and false otherwise
         * If the id already exists returns false and does NOT replace url
         */
        public bool TryAddServer(int id, string url) {
            return servers.TryAdd(id, url);
        }

        /*
         * Trys to find the server with the given id
         * Returns true if a server with given id exists and writes the value in string.
         * Otherwise, returns false and url is set to null
         */
        public bool TryFindServer(int id, out string url) {
            return servers.TryGetValue(id, out url);
        }

        public void RemoveServer(int id) {
            servers.Remove(id);
        }

        /*
         * Adds a client with the given id and url
         * Returns true if successfull and false otherwise
         * If the id already exists returns false and does NOT replace url
         */
        public bool TryAddClient(int id, string url) {
            return clients.TryAdd(id, url);
        }

        /*
         * Trys to find the client with the given id
         * Returns true if a client with given id exists and writes the value in string.
         * Otherwise, returns false and url is set to null
         */
        public bool TryFindClient(int id, out string url) {
            return clients.TryGetValue(id, out url);
        }

        public void RemoveClient(int id) {
            clients.Remove(id);
        }
    }
}
