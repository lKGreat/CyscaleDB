namespace CyscaleDB.Core.Common;

/// <summary>
/// Represents the result of an operation that can either succeed or fail.
/// </summary>
/// <typeparam name="T">The type of the success value.</typeparam>
public readonly struct Result<T>
{
    private readonly T? _value;
    private readonly CyscaleException? _error;

    /// <summary>
    /// Whether this result represents a success.
    /// </summary>
    public bool IsSuccess { get; }

    /// <summary>
    /// Whether this result represents a failure.
    /// </summary>
    public bool IsFailure => !IsSuccess;

    /// <summary>
    /// Gets the success value. Throws if this is a failure result.
    /// </summary>
    public T Value => IsSuccess
        ? _value!
        : throw new InvalidOperationException($"Cannot access Value on a failed result: {_error?.Message}");

    /// <summary>
    /// Gets the error. Throws if this is a success result.
    /// </summary>
    public CyscaleException Error => IsFailure
        ? _error!
        : throw new InvalidOperationException("Cannot access Error on a successful result");

    private Result(T? value, CyscaleException? error, bool isSuccess)
    {
        _value = value;
        _error = error;
        IsSuccess = isSuccess;
    }

    /// <summary>
    /// Creates a success result with the given value.
    /// </summary>
    public static Result<T> Success(T value) => new(value, null, true);

    /// <summary>
    /// Creates a failure result with the given error.
    /// </summary>
    public static Result<T> Failure(CyscaleException error) => new(default, error, false);

    /// <summary>
    /// Creates a failure result with the given error message.
    /// </summary>
    public static Result<T> Failure(string message, ErrorCode errorCode = ErrorCode.Unknown)
        => new(default, new CyscaleException(message, errorCode), false);

    /// <summary>
    /// Gets the value or a default if this is a failure.
    /// </summary>
    public T? GetValueOrDefault(T? defaultValue = default) => IsSuccess ? _value : defaultValue;

    /// <summary>
    /// Gets the value or throws the error if this is a failure.
    /// </summary>
    public T GetValueOrThrow()
    {
        if (IsFailure)
            throw _error!;
        return _value!;
    }

    /// <summary>
    /// Maps the success value to a new type.
    /// </summary>
    public Result<TNew> Map<TNew>(Func<T, TNew> mapper)
    {
        return IsSuccess
            ? Result<TNew>.Success(mapper(_value!))
            : Result<TNew>.Failure(_error!);
    }

    /// <summary>
    /// Flat maps the success value to a new result.
    /// </summary>
    public Result<TNew> FlatMap<TNew>(Func<T, Result<TNew>> mapper)
    {
        return IsSuccess ? mapper(_value!) : Result<TNew>.Failure(_error!);
    }

    /// <summary>
    /// Executes an action on the success value.
    /// </summary>
    public Result<T> OnSuccess(Action<T> action)
    {
        if (IsSuccess) action(_value!);
        return this;
    }

    /// <summary>
    /// Executes an action on the error.
    /// </summary>
    public Result<T> OnFailure(Action<CyscaleException> action)
    {
        if (IsFailure) action(_error!);
        return this;
    }

    /// <summary>
    /// Matches on success or failure.
    /// </summary>
    public TResult Match<TResult>(Func<T, TResult> onSuccess, Func<CyscaleException, TResult> onFailure)
    {
        return IsSuccess ? onSuccess(_value!) : onFailure(_error!);
    }

    public override string ToString()
    {
        return IsSuccess ? $"Success({_value})" : $"Failure({_error?.Message})";
    }

    // Implicit conversions
    public static implicit operator Result<T>(T value) => Success(value);
    public static implicit operator Result<T>(CyscaleException error) => Failure(error);
}

/// <summary>
/// Represents the result of an operation that can either succeed (with no value) or fail.
/// </summary>
public readonly struct Result
{
    private readonly CyscaleException? _error;

    /// <summary>
    /// Whether this result represents a success.
    /// </summary>
    public bool IsSuccess { get; }

    /// <summary>
    /// Whether this result represents a failure.
    /// </summary>
    public bool IsFailure => !IsSuccess;

    /// <summary>
    /// Gets the error. Throws if this is a success result.
    /// </summary>
    public CyscaleException Error => IsFailure
        ? _error!
        : throw new InvalidOperationException("Cannot access Error on a successful result");

    private Result(CyscaleException? error, bool isSuccess)
    {
        _error = error;
        IsSuccess = isSuccess;
    }

    /// <summary>
    /// A successful result.
    /// </summary>
    public static Result Success() => new(null, true);

    /// <summary>
    /// Creates a success result with the given value.
    /// </summary>
    public static Result<T> Success<T>(T value) => Result<T>.Success(value);

    /// <summary>
    /// Creates a failure result with the given error.
    /// </summary>
    public static Result Failure(CyscaleException error) => new(error, false);

    /// <summary>
    /// Creates a failure result with the given error message.
    /// </summary>
    public static Result Failure(string message, ErrorCode errorCode = ErrorCode.Unknown)
        => new(new CyscaleException(message, errorCode), false);

    /// <summary>
    /// Creates a failure result with the given error.
    /// </summary>
    public static Result<T> Failure<T>(CyscaleException error) => Result<T>.Failure(error);

    /// <summary>
    /// Creates a failure result with the given error message.
    /// </summary>
    public static Result<T> Failure<T>(string message, ErrorCode errorCode = ErrorCode.Unknown)
        => Result<T>.Failure(message, errorCode);

    /// <summary>
    /// Throws the error if this is a failure.
    /// </summary>
    public void ThrowIfFailure()
    {
        if (IsFailure)
            throw _error!;
    }

    /// <summary>
    /// Executes an action on success.
    /// </summary>
    public Result OnSuccess(Action action)
    {
        if (IsSuccess) action();
        return this;
    }

    /// <summary>
    /// Executes an action on the error.
    /// </summary>
    public Result OnFailure(Action<CyscaleException> action)
    {
        if (IsFailure) action(_error!);
        return this;
    }

    /// <summary>
    /// Matches on success or failure.
    /// </summary>
    public TResult Match<TResult>(Func<TResult> onSuccess, Func<CyscaleException, TResult> onFailure)
    {
        return IsSuccess ? onSuccess() : onFailure(_error!);
    }

    public override string ToString()
    {
        return IsSuccess ? "Success" : $"Failure({_error?.Message})";
    }

    // Implicit conversion from exception
    public static implicit operator Result(CyscaleException error) => Failure(error);
}
