﻿using System;
namespace Common.Utils {
    public class HttpURLs {
        private HttpURLs() {
        }

        public static bool TryParseHost(string url, out string host) {
            host = null;
            Uri uri;
            try {
                uri = new Uri(url);
            } catch (UriFormatException) {
                return false;
            }
            if (!uri.Scheme.Equals("http")) {
                return false;
            }
            host = uri.Host;
            return true;
        }

        public static bool TryParseHostAndPort(string url, out string host, out int port) {
            host = null;
            port = 0;

            Uri uri;
            try {
                uri = new Uri(url);
            } catch (UriFormatException) {
                return false;
            }
            if (!uri.Scheme.Equals("http")) {
                return false;
            }
            host = uri.Host;
            port = uri.Port;
            return true;
        }

        public static string FromHostAndPort(string host, int port) {
            return $"http://{host}:{port}";
        }
    }
}
