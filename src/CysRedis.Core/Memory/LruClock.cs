namespace CysRedis.Core.Memory;

/// <summary>
/// Redis-style LRU clock implementation.
/// Uses a 24-bit clock with 1-second resolution to minimize memory usage.
/// </summary>
public static class LruClock
{
    private static uint _clock;
    private const int LruBits = 24;
    private const uint LruClockMax = (1 << LruBits) - 1;
    private const int LruClockResolution = 1000; // 1 second resolution in milliseconds
    
    /// <summary>
    /// Gets the current LRU clock value (24 bits).
    /// </summary>
    public static uint GetClock() => _clock;
    
    /// <summary>
    /// Updates the clock. Should be called periodically (e.g., once per second).
    /// </summary>
    public static void UpdateClock()
    {
        _clock = (uint)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 
            LruClockResolution) & LruClockMax;
    }
    
    /// <summary>
    /// Calculates the idle time in seconds given an object's LRU timestamp.
    /// </summary>
    public static long EstimateIdleTime(uint objectLru)
    {
        var currentClock = _clock;
        long idleTime;
        
        if (currentClock >= objectLru)
        {
            idleTime = currentClock - objectLru;
        }
        else
        {
            // Clock wrapped around
            idleTime = (LruClockMax - objectLru) + currentClock;
        }
        
        return idleTime; // In seconds
    }
    
    /// <summary>
    /// Starts a background task to update the clock periodically.
    /// </summary>
    public static Task StartClockUpdater(CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                UpdateClock();
                await Task.Delay(1000, cancellationToken); // Update every second
            }
        }, cancellationToken);
    }
}
