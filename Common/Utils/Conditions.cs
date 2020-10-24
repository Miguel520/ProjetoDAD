using System;
namespace Common.Utils {

    /*
     * Class to check conditions throughout the code
     */
    public class Conditions {
        private Conditions() {
        }

        public static void AssertTrue(bool value, Action falseAction) {
            if (!value) {
                falseAction.Invoke();
            }
        }

        /*
         * Check if argument satisfies a condition
         * Throws ArgumentException if the argument does not
         * satisfy the the given condition
         */
        public static void AssertArgument(bool value) {
            if (!value) {
                throw new ArgumentException();
            }
        }

        /*
         * Check if state of method is correct
         * Throws InvalidOperationException if the value is false
         */
        public static void AssertState(bool value) {
            if (!value) {
                throw new InvalidOperationException();
            }
        }
    }
}
