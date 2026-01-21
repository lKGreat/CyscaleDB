using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using CysRedis.Core.Common;

namespace CysRedis.Core.Security;

/// <summary>
/// Handles TLS encryption for Redis connections.
/// </summary>
public sealed class TlsHandler : IDisposable
{
    private readonly TlsOptions _options;
    private readonly X509Certificate2? _serverCertificate;
    private readonly X509Certificate2Collection? _caCertificates;
    private bool _disposed;

    // Statistics
    private long _handshakeCount;
    private long _handshakeErrors;

    /// <summary>
    /// Gets whether TLS is enabled.
    /// </summary>
    public bool IsEnabled => _options.Enabled;

    /// <summary>
    /// Gets the TLS port.
    /// </summary>
    public int Port => _options.Port;

    /// <summary>
    /// Gets the number of successful TLS handshakes.
    /// </summary>
    public long HandshakeCount => Interlocked.Read(ref _handshakeCount);

    /// <summary>
    /// Gets the number of TLS handshake errors.
    /// </summary>
    public long HandshakeErrors => Interlocked.Read(ref _handshakeErrors);

    /// <summary>
    /// Creates a new TLS handler.
    /// </summary>
    public TlsHandler(TlsOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (_options.Enabled)
        {
            _options.Validate();

            // Load server certificate
            _serverCertificate = LoadCertificate(
                _options.CertificatePath!,
                _options.CertificatePassword);

            // Load CA certificates if specified
            if (!string.IsNullOrWhiteSpace(_options.CaCertificatePath))
            {
                _caCertificates = new X509Certificate2Collection();
                _caCertificates.Import(_options.CaCertificatePath);
            }

            Logger.Info("TLS handler initialized with certificate: {0}",
                _serverCertificate.Subject);
        }
    }

    /// <summary>
    /// Performs TLS handshake and wraps the stream with SslStream.
    /// </summary>
    /// <param name="networkStream">The underlying network stream.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The TLS-wrapped stream.</returns>
    public async Task<SslStream> AuthenticateAsServerAsync(
        Stream networkStream,
        CancellationToken cancellationToken = default)
    {
        if (!_options.Enabled || _serverCertificate == null)
        {
            throw new InvalidOperationException("TLS is not enabled");
        }

        var sslStream = new SslStream(
            networkStream,
            leaveInnerStreamOpen: false,
            userCertificateValidationCallback: ValidateClientCertificate,
            userCertificateSelectionCallback: null);

        try
        {
            using var timeoutCts = new CancellationTokenSource(_options.HandshakeTimeoutMs);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, timeoutCts.Token);

            var authOptions = new SslServerAuthenticationOptions
            {
                ServerCertificate = _serverCertificate,
                ClientCertificateRequired = _options.RequireClientCertificate,
                EnabledSslProtocols = _options.Protocols,
                CertificateRevocationCheckMode = _options.CheckCertificateRevocation
                    ? X509RevocationMode.Online
                    : X509RevocationMode.NoCheck
            };

            await sslStream.AuthenticateAsServerAsync(authOptions, linkedCts.Token);

            Interlocked.Increment(ref _handshakeCount);

            Logger.Debug("TLS handshake completed: Protocol={0}, CipherAlgorithm={1}",
                sslStream.SslProtocol,
                sslStream.CipherAlgorithm);

            return sslStream;
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _handshakeErrors);
            Logger.Error("TLS handshake failed", ex);

            sslStream.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Validates the client certificate.
    /// </summary>
    private bool ValidateClientCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        // If client certificate is not required and none provided, allow
        if (!_options.RequireClientCertificate && certificate == null)
        {
            return true;
        }

        // Allow self-signed certificates if configured (for development)
        if (_options.AllowSelfSignedCertificates)
        {
            return true;
        }

        // Check for policy errors
        if (sslPolicyErrors == SslPolicyErrors.None)
        {
            return true;
        }

        // Log the error
        Logger.Warning("Client certificate validation failed: {0}", sslPolicyErrors);

        // If we have CA certificates, validate against them
        if (_caCertificates != null && certificate != null && chain != null)
        {
            chain.ChainPolicy.ExtraStore.AddRange(_caCertificates);
            chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

            if (chain.Build(new X509Certificate2(certificate)))
            {
                // Check if the root certificate is in our CA store
                var root = chain.ChainElements[^1].Certificate;
                foreach (var ca in _caCertificates)
                {
                    if (root.Thumbprint == ca.Thumbprint)
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Loads a certificate from file.
    /// </summary>
    private static X509Certificate2 LoadCertificate(string path, string? password)
    {
        return new X509Certificate2(
            path,
            password,
            X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.PersistKeySet);
    }

    /// <summary>
    /// Gets TLS handler statistics.
    /// </summary>
    public TlsHandlerStats GetStats()
    {
        return new TlsHandlerStats
        {
            Enabled = _options.Enabled,
            HandshakeCount = HandshakeCount,
            HandshakeErrors = HandshakeErrors,
            Protocol = _options.Protocols.ToString(),
            RequireClientCertificate = _options.RequireClientCertificate
        };
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _serverCertificate?.Dispose();
        }
    }
}

/// <summary>
/// TLS handler statistics.
/// </summary>
public sealed class TlsHandlerStats
{
    public bool Enabled { get; init; }
    public long HandshakeCount { get; init; }
    public long HandshakeErrors { get; init; }
    public string Protocol { get; init; } = "";
    public bool RequireClientCertificate { get; init; }
}
