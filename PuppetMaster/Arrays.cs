using System;
namespace Utils {
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
    }
}
