using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Interface for text tokenizers used by the full-text index.
/// </summary>
public interface ITokenizer
{
    /// <summary>
    /// Tokenizes the input text into a sequence of tokens.
    /// </summary>
    /// <param name="text">The text to tokenize</param>
    /// <returns>Sequence of tokens</returns>
    IEnumerable<Token> Tokenize(string text);
}

/// <summary>
/// Represents a token extracted from text.
/// </summary>
public sealed class Token
{
    /// <summary>
    /// The normalized term (lowercase, stemmed, etc.).
    /// </summary>
    public string Term { get; }

    /// <summary>
    /// The position of this token in the original text.
    /// </summary>
    public int Position { get; }

    /// <summary>
    /// The start offset in the original text.
    /// </summary>
    public int StartOffset { get; }

    /// <summary>
    /// The length in the original text.
    /// </summary>
    public int Length { get; }

    public Token(string term, int position, int startOffset, int length)
    {
        Term = term;
        Position = position;
        StartOffset = startOffset;
        Length = length;
    }
}

/// <summary>
/// A simple tokenizer that splits text on whitespace and punctuation.
/// Supports stopword filtering and basic normalization.
/// </summary>
public sealed class SimpleTokenizer : ITokenizer
{
    private static readonly HashSet<string> DefaultStopwords = new(StringComparer.OrdinalIgnoreCase)
    {
        "a", "an", "the", "and", "or", "but", "is", "are", "was", "were",
        "be", "been", "being", "have", "has", "had", "do", "does", "did",
        "will", "would", "could", "should", "may", "might", "must", "shall",
        "of", "at", "by", "for", "with", "about", "against", "between", "into",
        "through", "during", "before", "after", "above", "below", "to", "from",
        "up", "down", "in", "out", "on", "off", "over", "under", "again",
        "further", "then", "once", "here", "there", "when", "where", "why",
        "how", "all", "each", "few", "more", "most", "other", "some", "such",
        "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very",
        "s", "t", "can", "just", "don", "now"
    };

    private readonly HashSet<string> _stopwords;
    private readonly bool _filterStopwords;
    private readonly int _minTokenLength;

    /// <summary>
    /// Creates a new simple tokenizer with default settings.
    /// </summary>
    public SimpleTokenizer(bool filterStopwords = true, int minTokenLength = 2)
    {
        _filterStopwords = filterStopwords;
        _minTokenLength = minTokenLength;
        _stopwords = DefaultStopwords;
    }

    /// <summary>
    /// Creates a new simple tokenizer with custom stopwords.
    /// </summary>
    public SimpleTokenizer(IEnumerable<string> stopwords, int minTokenLength = 2)
    {
        _filterStopwords = true;
        _minTokenLength = minTokenLength;
        _stopwords = new HashSet<string>(stopwords, StringComparer.OrdinalIgnoreCase);
    }

    public IEnumerable<Token> Tokenize(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            yield break;

        int position = 0;
        int startOffset = 0;
        var tokenBuilder = new System.Text.StringBuilder();

        for (int i = 0; i <= text.Length; i++)
        {
            char c = i < text.Length ? text[i] : ' ';

            if (char.IsLetterOrDigit(c))
            {
                if (tokenBuilder.Length == 0)
                {
                    startOffset = i;
                }
                tokenBuilder.Append(char.ToLowerInvariant(c));
            }
            else if (tokenBuilder.Length > 0)
            {
                var term = tokenBuilder.ToString();
                tokenBuilder.Clear();

                // Apply filters
                if (term.Length >= _minTokenLength)
                {
                    if (!_filterStopwords || !_stopwords.Contains(term))
                    {
                        yield return new Token(term, position, startOffset, i - startOffset);
                        position++;
                    }
                }
            }
        }
    }
}

/// <summary>
/// A full-text index implementation using an inverted index structure.
/// Supports adding documents, searching, and relevance ranking using TF-IDF.
/// </summary>
public sealed class FullTextIndex : IDisposable
{
    private readonly Dictionary<string, PostingList> _invertedIndex;
    private readonly Dictionary<int, DocumentInfo> _documents;
    private readonly ITokenizer _tokenizer;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private bool _disposed;

    /// <summary>
    /// Gets the number of documents in the index.
    /// </summary>
    public int DocumentCount
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _documents.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the number of unique terms in the index.
    /// </summary>
    public int TermCount
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _invertedIndex.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Creates a new full-text index with the default tokenizer.
    /// </summary>
    public FullTextIndex() : this(new SimpleTokenizer())
    {
    }

    /// <summary>
    /// Creates a new full-text index with a custom tokenizer.
    /// </summary>
    public FullTextIndex(ITokenizer tokenizer)
    {
        _tokenizer = tokenizer;
        _invertedIndex = new Dictionary<string, PostingList>(StringComparer.OrdinalIgnoreCase);
        _documents = new Dictionary<int, DocumentInfo>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<FullTextIndex>();
    }

    /// <summary>
    /// Adds a document to the index.
    /// </summary>
    /// <param name="documentId">Unique identifier for the document</param>
    /// <param name="text">The text content to index</param>
    public void AddDocument(int documentId, string text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return;

        var tokens = _tokenizer.Tokenize(text).ToList();
        if (tokens.Count == 0)
            return;

        _lock.EnterWriteLock();
        try
        {
            // Remove existing document if present
            RemoveDocumentInternal(documentId);

            // Create document info
            var docInfo = new DocumentInfo(documentId, tokens.Count);
            _documents[documentId] = docInfo;

            // Count term frequencies
            var termFrequencies = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
            foreach (var token in tokens)
            {
                if (!termFrequencies.TryGetValue(token.Term, out var positions))
                {
                    positions = new List<int>();
                    termFrequencies[token.Term] = positions;
                }
                positions.Add(token.Position);
            }

            // Add to inverted index
            foreach (var kvp in termFrequencies)
            {
                if (!_invertedIndex.TryGetValue(kvp.Key, out var postingList))
                {
                    postingList = new PostingList();
                    _invertedIndex[kvp.Key] = postingList;
                }

                postingList.Add(documentId, kvp.Value);
            }

            _logger.Trace("Added document {0} with {1} tokens", documentId, tokens.Count);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes a document from the index.
    /// </summary>
    public bool RemoveDocument(int documentId)
    {
        _lock.EnterWriteLock();
        try
        {
            return RemoveDocumentInternal(documentId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private bool RemoveDocumentInternal(int documentId)
    {
        if (!_documents.Remove(documentId))
            return false;

        // Remove from all posting lists
        var emptyTerms = new List<string>();
        foreach (var kvp in _invertedIndex)
        {
            kvp.Value.Remove(documentId);
            if (kvp.Value.Count == 0)
            {
                emptyTerms.Add(kvp.Key);
            }
        }

        // Clean up empty posting lists
        foreach (var term in emptyTerms)
        {
            _invertedIndex.Remove(term);
        }

        return true;
    }

    /// <summary>
    /// Searches for documents matching the query.
    /// Returns documents ranked by relevance (TF-IDF score).
    /// </summary>
    /// <param name="query">The search query</param>
    /// <param name="maxResults">Maximum number of results to return</param>
    /// <returns>List of document references with relevance scores</returns>
    public List<SearchResult> Search(string query, int maxResults = 100)
    {
        if (string.IsNullOrWhiteSpace(query))
            return new List<SearchResult>();

        var queryTokens = _tokenizer.Tokenize(query).ToList();
        if (queryTokens.Count == 0)
            return new List<SearchResult>();

        _lock.EnterReadLock();
        try
        {
            if (_documents.Count == 0)
                return new List<SearchResult>();

            // Get unique query terms
            var queryTerms = queryTokens.Select(t => t.Term).Distinct().ToList();

            // Calculate scores using TF-IDF
            var scores = new Dictionary<int, double>();
            int totalDocuments = _documents.Count;

            foreach (var term in queryTerms)
            {
                if (!_invertedIndex.TryGetValue(term, out var postingList))
                    continue;

                // IDF: log(N / df)
                double idf = Math.Log((double)totalDocuments / postingList.Count);

                foreach (var posting in postingList.GetPostings())
                {
                    var docInfo = _documents[posting.DocumentId];
                    
                    // TF: frequency / document length (normalized)
                    double tf = (double)posting.Positions.Count / docInfo.TokenCount;
                    
                    // TF-IDF score
                    double score = tf * idf;

                    if (!scores.TryGetValue(posting.DocumentId, out var currentScore))
                    {
                        scores[posting.DocumentId] = score;
                    }
                    else
                    {
                        scores[posting.DocumentId] = currentScore + score;
                    }
                }
            }

            // Sort by score and return top results
            return scores
                .OrderByDescending(kvp => kvp.Value)
                .Take(maxResults)
                .Select(kvp => new SearchResult(kvp.Key, kvp.Value))
                .ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Searches using boolean operators (AND, OR, NOT).
    /// </summary>
    /// <param name="query">The search query with boolean operators</param>
    /// <param name="mode">The search mode</param>
    /// <param name="maxResults">Maximum number of results</param>
    /// <returns>List of matching documents</returns>
    public List<SearchResult> SearchBoolean(string query, BooleanSearchMode mode, int maxResults = 100)
    {
        if (string.IsNullOrWhiteSpace(query))
            return new List<SearchResult>();

        var queryTokens = _tokenizer.Tokenize(query).ToList();
        if (queryTokens.Count == 0)
            return new List<SearchResult>();

        _lock.EnterReadLock();
        try
        {
            if (_documents.Count == 0)
                return new List<SearchResult>();

            var queryTerms = queryTokens.Select(t => t.Term).Distinct().ToList();
            HashSet<int>? resultSet = null;

            foreach (var term in queryTerms)
            {
                var matchingDocs = new HashSet<int>();
                if (_invertedIndex.TryGetValue(term, out var postingList))
                {
                    foreach (var posting in postingList.GetPostings())
                    {
                        matchingDocs.Add(posting.DocumentId);
                    }
                }

                if (resultSet == null)
                {
                    resultSet = matchingDocs;
                }
                else
                {
                    switch (mode)
                    {
                        case BooleanSearchMode.And:
                            resultSet.IntersectWith(matchingDocs);
                            break;
                        case BooleanSearchMode.Or:
                            resultSet.UnionWith(matchingDocs);
                            break;
                    }
                }
            }

            if (resultSet == null || resultSet.Count == 0)
                return new List<SearchResult>();

            // Calculate relevance scores for matching documents
            return Search(query, maxResults)
                .Where(r => resultSet.Contains(r.DocumentId))
                .ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Calculates the relevance score for a document against a query.
    /// </summary>
    public double CalculateRelevance(int documentId, string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return 0;

        var queryTokens = _tokenizer.Tokenize(query).ToList();
        if (queryTokens.Count == 0)
            return 0;

        _lock.EnterReadLock();
        try
        {
            if (!_documents.TryGetValue(documentId, out var docInfo))
                return 0;

            var queryTerms = queryTokens.Select(t => t.Term).Distinct().ToList();
            double score = 0;
            int totalDocuments = _documents.Count;

            foreach (var term in queryTerms)
            {
                if (!_invertedIndex.TryGetValue(term, out var postingList))
                    continue;

                var posting = postingList.GetPosting(documentId);
                if (posting == null)
                    continue;

                double idf = Math.Log((double)totalDocuments / postingList.Count);
                double tf = (double)posting.Positions.Count / docInfo.TokenCount;
                score += tf * idf;
            }

            return score;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Clears all documents from the index.
    /// </summary>
    public void Clear()
    {
        _lock.EnterWriteLock();
        try
        {
            _invertedIndex.Clear();
            _documents.Clear();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _lock.Dispose();
    }

    #region Nested Types

    private sealed class PostingList
    {
        private readonly Dictionary<int, Posting> _postings = new();

        public int Count => _postings.Count;

        public void Add(int documentId, List<int> positions)
        {
            _postings[documentId] = new Posting(documentId, positions);
        }

        public void Remove(int documentId)
        {
            _postings.Remove(documentId);
        }

        public Posting? GetPosting(int documentId)
        {
            return _postings.GetValueOrDefault(documentId);
        }

        public IEnumerable<Posting> GetPostings()
        {
            return _postings.Values;
        }
    }

    private sealed class Posting
    {
        public int DocumentId { get; }
        public List<int> Positions { get; }

        public Posting(int documentId, List<int> positions)
        {
            DocumentId = documentId;
            Positions = positions;
        }
    }

    private sealed class DocumentInfo
    {
        public int DocumentId { get; }
        public int TokenCount { get; }

        public DocumentInfo(int documentId, int tokenCount)
        {
            DocumentId = documentId;
            TokenCount = tokenCount;
        }
    }

    #endregion
}

/// <summary>
/// A search result from the full-text index.
/// </summary>
public sealed class SearchResult
{
    /// <summary>
    /// The document ID.
    /// </summary>
    public int DocumentId { get; }

    /// <summary>
    /// The relevance score (higher is more relevant).
    /// </summary>
    public double Score { get; }

    public SearchResult(int documentId, double score)
    {
        DocumentId = documentId;
        Score = score;
    }
}

/// <summary>
/// Mode for boolean search operations.
/// </summary>
public enum BooleanSearchMode
{
    /// <summary>
    /// All terms must be present (AND).
    /// </summary>
    And,

    /// <summary>
    /// Any term can be present (OR).
    /// </summary>
    Or
}

/// <summary>
/// Mode for MATCH...AGAINST syntax.
/// </summary>
public enum FullTextSearchMode
{
    /// <summary>
    /// Natural language mode (default).
    /// </summary>
    NaturalLanguage,

    /// <summary>
    /// Boolean mode with operators.
    /// </summary>
    Boolean,

    /// <summary>
    /// Query expansion mode.
    /// </summary>
    QueryExpansion
}
