using System;
namespace Common.Utils {
    public class Arrays {
        public static T[] Slice<T>(T[] source, int begin, int end) {
            int size = end - begin;
            if (size < 0) {
                throw new ArgumentException();
            }
            T[] result = new T[size];
            Array.Copy(source, begin, result, 0, size);
            return result;
        }

        public static string ToString<T>(T[] array) {
            foreach (T el in array) {
                Console.WriteLine(el);
            }
            return $"[{string.Join(", ", array)}]";
        }

        public static bool IsEmpty<T>(T[] array) {
            return array.Length == 0;
        }
    }
}
