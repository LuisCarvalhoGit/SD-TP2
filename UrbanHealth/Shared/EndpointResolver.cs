using System;
using System.IO;
using System.Net;

namespace Shared;

public static class EndpointResolver {
    public static string ResolveHost(string? configuredHost) {
        string host = string.IsNullOrWhiteSpace(configuredHost)
            ? "local"
            : configuredHost.Trim();

        if (IsLocalAlias(host) || (IsRunningInContainer() && IsLoopbackHost(host))) {
            return IsRunningInContainer() ? "host.docker.internal" : "127.0.0.1";
        }

        return host;
    }

    public static string ResolveHttpUrl(string? configuredUrl, string fallbackUrl) {
        string rawUrl = string.IsNullOrWhiteSpace(configuredUrl) ? fallbackUrl : configuredUrl.Trim();
        if (!Uri.TryCreate(rawUrl, UriKind.Absolute, out var uri)) return rawUrl;

        string resolvedHost = ResolveHost(uri.Host);
        var builder = new UriBuilder(uri) { Host = resolvedHost };
        return builder.Uri.ToString();
    }

    private static bool IsLocalAlias(string host) {
        return host.Equals("local", StringComparison.OrdinalIgnoreCase) ||
               host.Equals("same-host", StringComparison.OrdinalIgnoreCase) ||
               host.Equals("host", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsLoopbackHost(string host) {
        if (host.Equals("localhost", StringComparison.OrdinalIgnoreCase)) return true;
        return IPAddress.TryParse(host, out var ip) && IPAddress.IsLoopback(ip);
    }

    private static bool IsRunningInContainer() {
        return string.Equals(Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER"), "true", StringComparison.OrdinalIgnoreCase) ||
               File.Exists("/.dockerenv");
    }
}
