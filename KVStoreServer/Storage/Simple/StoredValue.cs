using System;
using System.Threading;

namespace KVStoreServer.Storage.Simple {

    /*
     * Class to store a value in a key value store
     * The value can be locked and unlocked
     * The value can only be updated if it was locked before
     */
    public class StoredValue {

        private bool locked = false;
        private string value = null;

        public StoredValue() {
        }

        public string Value {
            set {
                lock (this) {
                    if (!locked) {
                        throw new InvalidOperationException();
                    }
                    this.value = value;
                    // Unlocks the object for posterior operations
                    locked = false;
                    Monitor.PulseAll(this);
                }
            }
            get {
                lock (this) {
                    while (locked) {
                        Monitor.Wait(this);
                    }
                    return value;
                }
            }
        }

        /*
         * Locks an object to prepare a write
         */
        public void Lock() {
            lock (this) {
                locked = true;
            }
        }

        //just for debug purposes
        public StoredValueDto GetStoredValueDto() {
            return new StoredValueDto {
                IsLocked = locked,
                Value = value
            };
        }
    }
}
