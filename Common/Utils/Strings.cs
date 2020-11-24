namespace Common.Utils {
    public class Strings {

        private Strings() {}

        public static bool LessThan(string first, string second) {
            return string.Compare(first, second) < 0;
        }
    }
}
