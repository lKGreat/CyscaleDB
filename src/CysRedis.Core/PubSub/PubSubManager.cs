using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.PubSub;

/// <summary>
/// Manages Pub/Sub subscriptions and message broadcasting.
/// </summary>
public class PubSubManager
{
    private readonly ConcurrentDictionary<string, HashSet<RedisClient>> _channelSubscribers;
    private readonly ConcurrentDictionary<string, HashSet<RedisClient>> _patternSubscribers;
    private readonly ConcurrentDictionary<RedisClient, HashSet<string>> _clientChannels;
    private readonly ConcurrentDictionary<RedisClient, HashSet<string>> _clientPatterns;
    private readonly object _lock = new();

    public PubSubManager()
    {
        _channelSubscribers = new ConcurrentDictionary<string, HashSet<RedisClient>>(StringComparer.Ordinal);
        _patternSubscribers = new ConcurrentDictionary<string, HashSet<RedisClient>>(StringComparer.Ordinal);
        _clientChannels = new ConcurrentDictionary<RedisClient, HashSet<string>>();
        _clientPatterns = new ConcurrentDictionary<RedisClient, HashSet<string>>();
    }

    /// <summary>
    /// Subscribes a client to channels.
    /// </summary>
    public async Task SubscribeAsync(RedisClient client, string[] channels, CancellationToken cancellationToken)
    {
        var clientChannels = _clientChannels.GetOrAdd(client, _ => new HashSet<string>(StringComparer.Ordinal));

        foreach (var channel in channels)
        {
            lock (_lock)
            {
                var subscribers = _channelSubscribers.GetOrAdd(channel, _ => new HashSet<RedisClient>());
                subscribers.Add(client);
                clientChannels.Add(channel);
            }

            client.InPubSubMode = true;

            // Send subscription confirmation
            var count = GetSubscriptionCount(client);
            await client.WriteResponseAsync(RespValue.Array(
                RespValue.BulkString("subscribe"),
                RespValue.BulkString(channel),
                new RespValue(count)
            ), cancellationToken);
        }
    }

    /// <summary>
    /// Unsubscribes a client from channels.
    /// </summary>
    public async Task UnsubscribeAsync(RedisClient client, string[]? channels, CancellationToken cancellationToken)
    {
        if (!_clientChannels.TryGetValue(client, out var clientChannels))
            return;

        var toUnsubscribe = channels == null || channels.Length == 0 
            ? clientChannels.ToArray() 
            : channels;

        foreach (var channel in toUnsubscribe)
        {
            lock (_lock)
            {
                if (_channelSubscribers.TryGetValue(channel, out var subscribers))
                {
                    subscribers.Remove(client);
                    if (subscribers.Count == 0)
                        _channelSubscribers.TryRemove(channel, out _);
                }
                clientChannels.Remove(channel);
            }

            var count = GetSubscriptionCount(client);
            await client.WriteResponseAsync(RespValue.Array(
                RespValue.BulkString("unsubscribe"),
                RespValue.BulkString(channel),
                new RespValue(count)
            ), cancellationToken);

            if (count == 0)
                client.InPubSubMode = false;
        }
    }

    /// <summary>
    /// Subscribes a client to patterns.
    /// </summary>
    public async Task PSubscribeAsync(RedisClient client, string[] patterns, CancellationToken cancellationToken)
    {
        var clientPatterns = _clientPatterns.GetOrAdd(client, _ => new HashSet<string>(StringComparer.Ordinal));

        foreach (var pattern in patterns)
        {
            lock (_lock)
            {
                var subscribers = _patternSubscribers.GetOrAdd(pattern, _ => new HashSet<RedisClient>());
                subscribers.Add(client);
                clientPatterns.Add(pattern);
            }

            client.InPubSubMode = true;

            var count = GetSubscriptionCount(client);
            await client.WriteResponseAsync(RespValue.Array(
                RespValue.BulkString("psubscribe"),
                RespValue.BulkString(pattern),
                new RespValue(count)
            ), cancellationToken);
        }
    }

    /// <summary>
    /// Unsubscribes a client from patterns.
    /// </summary>
    public async Task PUnsubscribeAsync(RedisClient client, string[]? patterns, CancellationToken cancellationToken)
    {
        if (!_clientPatterns.TryGetValue(client, out var clientPatterns))
            return;

        var toUnsubscribe = patterns == null || patterns.Length == 0 
            ? clientPatterns.ToArray() 
            : patterns;

        foreach (var pattern in toUnsubscribe)
        {
            lock (_lock)
            {
                if (_patternSubscribers.TryGetValue(pattern, out var subscribers))
                {
                    subscribers.Remove(client);
                    if (subscribers.Count == 0)
                        _patternSubscribers.TryRemove(pattern, out _);
                }
                clientPatterns.Remove(pattern);
            }

            var count = GetSubscriptionCount(client);
            await client.WriteResponseAsync(RespValue.Array(
                RespValue.BulkString("punsubscribe"),
                RespValue.BulkString(pattern),
                new RespValue(count)
            ), cancellationToken);

            if (count == 0)
                client.InPubSubMode = false;
        }
    }

    /// <summary>
    /// Publishes a message to a channel.
    /// </summary>
    public async Task<int> PublishAsync(string channel, string message, CancellationToken cancellationToken)
    {
        int receivers = 0;

        // Direct channel subscribers
        if (_channelSubscribers.TryGetValue(channel, out var subscribers))
        {
            RedisClient[] subs;
            lock (_lock)
            {
                subs = subscribers.ToArray();
            }

            foreach (var client in subs)
            {
                try
                {
                    await client.WriteResponseAsync(RespValue.Array(
                        RespValue.BulkString("message"),
                        RespValue.BulkString(channel),
                        RespValue.BulkString(message)
                    ), cancellationToken);
                    receivers++;
                }
                catch
                {
                    // Client disconnected
                }
            }
        }

        // Pattern subscribers
        foreach (var (pattern, patternSubs) in _patternSubscribers)
        {
            if (!MatchPattern(pattern, channel))
                continue;

            RedisClient[] subs;
            lock (_lock)
            {
                subs = patternSubs.ToArray();
            }

            foreach (var client in subs)
            {
                try
                {
                    await client.WriteResponseAsync(RespValue.Array(
                        RespValue.BulkString("pmessage"),
                        RespValue.BulkString(pattern),
                        RespValue.BulkString(channel),
                        RespValue.BulkString(message)
                    ), cancellationToken);
                    receivers++;
                }
                catch
                {
                    // Client disconnected
                }
            }
        }

        return receivers;
    }

    /// <summary>
    /// Removes a client from all subscriptions.
    /// </summary>
    public void RemoveClient(RedisClient client)
    {
        lock (_lock)
        {
            if (_clientChannels.TryRemove(client, out var channels))
            {
                foreach (var channel in channels)
                {
                    if (_channelSubscribers.TryGetValue(channel, out var subscribers))
                    {
                        subscribers.Remove(client);
                        if (subscribers.Count == 0)
                            _channelSubscribers.TryRemove(channel, out _);
                    }
                }
            }

            if (_clientPatterns.TryRemove(client, out var patterns))
            {
                foreach (var pattern in patterns)
                {
                    if (_patternSubscribers.TryGetValue(pattern, out var subscribers))
                    {
                        subscribers.Remove(client);
                        if (subscribers.Count == 0)
                            _patternSubscribers.TryRemove(pattern, out _);
                    }
                }
            }
        }
    }

    /// <summary>
    /// Gets active channels matching a pattern.
    /// </summary>
    public IEnumerable<string> GetChannels(string? pattern = null)
    {
        var channels = _channelSubscribers.Keys;
        if (string.IsNullOrEmpty(pattern) || pattern == "*")
            return channels;

        return channels.Where(c => MatchPattern(pattern, c));
    }

    /// <summary>
    /// Gets subscriber count for a channel.
    /// </summary>
    public int GetChannelSubscriberCount(string channel)
    {
        return _channelSubscribers.TryGetValue(channel, out var subscribers) ? subscribers.Count : 0;
    }

    /// <summary>
    /// Gets total pattern subscription count.
    /// </summary>
    public int GetPatternCount()
    {
        return _patternSubscribers.Values.Sum(s => s.Count);
    }

    private int GetSubscriptionCount(RedisClient client)
    {
        int count = 0;
        if (_clientChannels.TryGetValue(client, out var channels))
            count += channels.Count;
        if (_clientPatterns.TryGetValue(client, out var patterns))
            count += patterns.Count;
        return count;
    }

    private static bool MatchPattern(string pattern, string value)
    {
        // Convert Redis glob pattern to regex
        var regexPattern = "^" + Regex.Escape(pattern)
            .Replace("\\*", ".*")
            .Replace("\\?", ".")
            .Replace("\\[", "[")
            .Replace("\\]", "]") + "$";

        return Regex.IsMatch(value, regexPattern);
    }
}
