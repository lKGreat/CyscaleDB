using System.Security.Authentication;

namespace CysRedis.Core.Security;

/// <summary>
/// TLS/SSL configuration options for Redis server.
/// </summary>
public class TlsOptions
{
    /// <summary>
    /// Whether TLS is enabled.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Path to the server certificate file (PFX/PKCS12 format).
    /// </summary>
    public string? CertificatePath { get; set; }

    /// <summary>
    /// Password for the certificate file.
    /// </summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Allowed SSL/TLS protocols.
    /// </summary>
    public SslProtocols Protocols { get; set; } = SslProtocols.Tls12 | SslProtocols.Tls13;

    /// <summary>
    /// Whether to require client certificate authentication.
    /// </summary>
    public bool RequireClientCertificate { get; set; } = false;

    /// <summary>
    /// Whether to check certificate revocation.
    /// </summary>
    public bool CheckCertificateRevocation { get; set; } = false;

    /// <summary>
    /// Timeout for TLS handshake in milliseconds.
    /// </summary>
    public int HandshakeTimeoutMs { get; set; } = 10000;

    /// <summary>
    /// Path to CA certificate for client certificate validation.
    /// </summary>
    public string? CaCertificatePath { get; set; }

    /// <summary>
    /// Whether to allow self-signed certificates.
    /// Only for development/testing.
    /// </summary>
    public bool AllowSelfSignedCertificates { get; set; } = false;

    /// <summary>
    /// TLS port (default: 6380 for Redis).
    /// </summary>
    public int Port { get; set; } = 6380;

    /// <summary>
    /// Validates the TLS options.
    /// </summary>
    public void Validate()
    {
        if (Enabled)
        {
            if (string.IsNullOrWhiteSpace(CertificatePath))
            {
                throw new InvalidOperationException("TLS is enabled but CertificatePath is not set");
            }

            if (!File.Exists(CertificatePath))
            {
                throw new FileNotFoundException($"Certificate file not found: {CertificatePath}");
            }
        }
    }
}
