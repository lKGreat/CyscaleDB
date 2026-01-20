using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution;

/// <summary>
/// Interface for query operators using the Volcano/Iterator model.
/// </summary>
public interface IOperator : IDisposable
{
    /// <summary>
    /// Opens the operator and initializes it for iteration.
    /// </summary>
    void Open();

    /// <summary>
    /// Gets the next row from this operator.
    /// Returns null when no more rows are available.
    /// </summary>
    Row? Next();

    /// <summary>
    /// Closes the operator and releases resources.
    /// </summary>
    void Close();

    /// <summary>
    /// Gets the schema of the output rows.
    /// </summary>
    TableSchema Schema { get; }
}

/// <summary>
/// Base class for operators providing common functionality.
/// </summary>
public abstract class OperatorBase : IOperator
{
    protected bool _isOpen;
    protected bool _isDisposed;

    public abstract TableSchema Schema { get; }

    public virtual void Open()
    {
        if (_isOpen)
            throw new InvalidOperationException("Operator is already open");
        _isOpen = true;
    }

    public abstract Row? Next();

    public virtual void Close()
    {
        _isOpen = false;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_isDisposed)
            return;

        if (disposing && _isOpen)
        {
            Close();
        }

        _isDisposed = true;
    }
}
