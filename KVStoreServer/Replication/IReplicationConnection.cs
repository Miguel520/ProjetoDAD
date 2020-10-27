using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KVStoreServer.Replication {

    /*
     * Interface defines technology agnostic methods 
     * required for a connection that support replication
     */
    public interface IReplicationConnection {

        public Task Lock(string partitionName, int objectId);

        public Task Write(string partitionName, int objectId, string objectValue);
    }
}
