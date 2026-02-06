using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// External sort operator that supports spilling to disk when the in-memory sort buffer
/// exceeds the configured limit. Implements a k-way merge sort strategy:
///
/// 1. Read rows from input into a memory buffer (limited by sortBufferSizeBytes)
/// 2. When the buffer is full, sort it in-memory and spill to a temporary file (a "run")
/// 3. After all input is consumed, perform a k-way merge of all sorted runs
///
/// For small datasets that fit in memory, this behaves identically to the in-memory OrderByOperator.
/// For large datasets (billions of rows), it gracefully spills to disk and uses minimal memory.
/// </summary>
public sealed class ExternalSortOperator : OperatorBase
{
    private readonly IOperator _input;
    private readonly List<SortKey> _sortKeys;
    private readonly long _sortBufferSizeBytes;
    private readonly string? _tempDirectory;
    private readonly RowComparer _comparer;

    // State during merge phase
    private List<SpillFile>? _spillFiles;
    private List<SpillFileReader>? _mergeReaders;
    private Row?[]? _mergeHeads;
    private List<Row>? _inMemoryRows;
    private int _inMemoryIndex;
    private bool _isMergePhase;
    private long _estimatedMemoryUsed;

    // Approximate row size estimation
    private const int EstimatedRowOverhead = 200; // bytes per row (conservative estimate)

    public override TableSchema Schema => _input.Schema;

    /// <summary>
    /// Creates a new ExternalSortOperator.
    /// </summary>
    /// <param name="input">The input operator providing unsorted rows.</param>
    /// <param name="sortKeys">The sort keys defining the ordering.</param>
    /// <param name="sortBufferSizeBytes">Maximum bytes for in-memory sort buffer before spilling.
    /// Defaults to 256 MB.</param>
    /// <param name="tempDirectory">Directory for temporary spill files. Defaults to system temp.</param>
    public ExternalSortOperator(
        IOperator input,
        List<SortKey> sortKeys,
        long sortBufferSizeBytes = MemoryBudgetManager.DefaultSortBufferSize,
        string? tempDirectory = null)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        _sortKeys = sortKeys ?? throw new ArgumentNullException(nameof(sortKeys));
        _sortBufferSizeBytes = sortBufferSizeBytes;
        _tempDirectory = tempDirectory;
        _comparer = new RowComparer(sortKeys);
    }

    public override void Open()
    {
        base.Open();
        _input.Open();

        _spillFiles = new List<SpillFile>();
        _inMemoryRows = new List<Row>();
        _estimatedMemoryUsed = 0;
        _inMemoryIndex = 0;
        _isMergePhase = false;

        // Phase 1: Read input, buffer in memory, spill when buffer is full
        Row? row;
        while ((row = _input.Next()) != null)
        {
            _inMemoryRows.Add(row);
            _estimatedMemoryUsed += EstimateRowSize(row);

            if (_estimatedMemoryUsed >= _sortBufferSizeBytes)
            {
                SpillCurrentBuffer();
            }
        }

        // Phase 2: Determine merge strategy
        if (_spillFiles.Count == 0)
        {
            // All data fits in memory - simple in-memory sort
            _inMemoryRows.Sort(_comparer);
            _isMergePhase = false;
        }
        else
        {
            // We have spill files. Spill remaining in-memory rows if any.
            if (_inMemoryRows.Count > 0)
            {
                SpillCurrentBuffer();
            }

            // Set up k-way merge
            SetupMerge();
            _isMergePhase = true;
        }
    }

    public override Row? Next()
    {
        if (_isMergePhase)
        {
            return MergeNext();
        }
        else
        {
            // In-memory mode
            if (_inMemoryRows == null || _inMemoryIndex >= _inMemoryRows.Count)
                return null;
            return _inMemoryRows[_inMemoryIndex++];
        }
    }

    public override void Close()
    {
        _input.Close();
        CleanupMerge();
        CleanupSpillFiles();
        _inMemoryRows = null;
        _inMemoryIndex = 0;
        _estimatedMemoryUsed = 0;
        _isMergePhase = false;
        base.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _input.Dispose();
            CleanupMerge();
            CleanupSpillFiles();
        }
        base.Dispose(disposing);
    }

    #region Spill Logic

    /// <summary>
    /// Sorts the current in-memory buffer and writes it to a spill file (a sorted "run").
    /// </summary>
    private void SpillCurrentBuffer()
    {
        if (_inMemoryRows == null || _inMemoryRows.Count == 0)
            return;

        // Sort the buffer in memory
        _inMemoryRows.Sort(_comparer);

        // Write to spill file
        var spillFile = new SpillFile(_input.Schema, _tempDirectory, "cyscale_sort");
        spillFile.OpenForWrite();

        foreach (var r in _inMemoryRows)
        {
            spillFile.WriteRow(r);
        }

        spillFile.FinishWriting();
        _spillFiles!.Add(spillFile);

        // Clear buffer
        _inMemoryRows.Clear();
        _estimatedMemoryUsed = 0;
    }

    #endregion

    #region K-Way Merge

    /// <summary>
    /// Sets up the k-way merge by opening readers for all spill files
    /// and reading the first row from each.
    /// </summary>
    private void SetupMerge()
    {
        _mergeReaders = new List<SpillFileReader>();
        _mergeHeads = new Row?[_spillFiles!.Count];

        for (int i = 0; i < _spillFiles.Count; i++)
        {
            var reader = _spillFiles[i].OpenForRead();
            _mergeReaders.Add(reader);
            _mergeHeads[i] = reader.ReadRow();
        }
    }

    /// <summary>
    /// Returns the next row from the k-way merge.
    /// Uses a simple linear scan to find the minimum (for small k, this is efficient).
    /// For very large k (>64 runs), a priority queue / loser tree would be more efficient.
    /// </summary>
    private Row? MergeNext()
    {
        if (_mergeHeads == null || _mergeReaders == null)
            return null;

        // Find the reader with the smallest current head
        int bestIdx = -1;
        Row? bestRow = null;

        for (int i = 0; i < _mergeHeads.Length; i++)
        {
            var head = _mergeHeads[i];
            if (head == null) continue;

            if (bestRow == null || _comparer.Compare(head, bestRow) < 0)
            {
                bestIdx = i;
                bestRow = head;
            }
        }

        if (bestIdx < 0)
            return null;

        // Advance the chosen reader
        _mergeHeads[bestIdx] = _mergeReaders[bestIdx].ReadRow();

        return bestRow;
    }

    #endregion

    #region Cleanup

    private void CleanupMerge()
    {
        if (_mergeReaders != null)
        {
            foreach (var reader in _mergeReaders)
                reader.Dispose();
            _mergeReaders = null;
        }
        _mergeHeads = null;
    }

    private void CleanupSpillFiles()
    {
        if (_spillFiles != null)
        {
            foreach (var file in _spillFiles)
                file.Dispose();
            _spillFiles = null;
        }
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Estimates the in-memory size of a row for budget tracking.
    /// </summary>
    private static long EstimateRowSize(Row row)
    {
        long size = EstimatedRowOverhead;
        foreach (var val in row.Values)
        {
            if (val.IsNull) continue;
            size += val.Type switch
            {
                DataType.TinyInt or DataType.Boolean => 1,
                DataType.SmallInt => 2,
                DataType.Int or DataType.Float or DataType.Date => 4,
                DataType.BigInt or DataType.Double or DataType.DateTime or DataType.Timestamp or DataType.Time => 8,
                DataType.Decimal => 16,
                DataType.VarChar or DataType.Char or DataType.Text or DataType.Json =>
                    24 + (val.AsString()?.Length ?? 0) * 2, // string object overhead + chars
                DataType.Blob => 24 + (val.AsBlob()?.Length ?? 0),
                _ => 8
            };
        }
        return size;
    }

    /// <summary>
    /// Comparer for rows based on sort keys. Reused from OrderByOperator pattern.
    /// </summary>
    private sealed class RowComparer : IComparer<Row>
    {
        private readonly List<SortKey> _sortKeys;

        public RowComparer(List<SortKey> sortKeys)
        {
            _sortKeys = sortKeys;
        }

        public int Compare(Row? x, Row? y)
        {
            if (x == null && y == null) return 0;
            if (x == null) return -1;
            if (y == null) return 1;

            foreach (var key in _sortKeys)
            {
                var valX = key.Expression.Evaluate(x);
                var valY = key.Expression.Evaluate(y);

                var cmp = CompareValues(valX, valY);

                if (cmp != 0)
                {
                    return key.Direction == SortDirection.Descending ? -cmp : cmp;
                }
            }

            return 0;
        }

        private static int CompareValues(DataValue x, DataValue y)
        {
            if (x.IsNull && y.IsNull) return 0;
            if (x.IsNull) return -1;
            if (y.IsNull) return 1;

            return x.Type switch
            {
                DataType.Int => x.AsInt().CompareTo(y.AsInt()),
                DataType.BigInt => x.AsBigInt().CompareTo(y.AsBigInt()),
                DataType.SmallInt => x.AsSmallInt().CompareTo(y.AsSmallInt()),
                DataType.TinyInt => x.AsTinyInt().CompareTo(y.AsTinyInt()),
                DataType.Float => x.AsFloat().CompareTo(y.AsFloat()),
                DataType.Double => x.AsDouble().CompareTo(y.AsDouble()),
                DataType.Decimal => x.AsDecimal().CompareTo(y.AsDecimal()),
                DataType.VarChar or DataType.Char or DataType.Text =>
                    string.Compare(x.AsString(), y.AsString(), StringComparison.Ordinal),
                DataType.DateTime or DataType.Timestamp => x.AsDateTime().CompareTo(y.AsDateTime()),
                DataType.Date => x.AsDate().CompareTo(y.AsDate()),
                DataType.Time => x.AsTime().CompareTo(y.AsTime()),
                DataType.Boolean => x.AsBoolean().CompareTo(y.AsBoolean()),
                _ => 0
            };
        }
    }

    #endregion
}
