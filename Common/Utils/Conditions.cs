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
    }
}
