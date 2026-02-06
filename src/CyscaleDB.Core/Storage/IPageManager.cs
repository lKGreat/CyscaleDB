namespace CyscaleDB.Core.Storage;

/// <summary>
/// Interface for page managers, supporting both single-file (PageManager)
/// and multi-file (MultiFilePageManager) implementations.
/// </summary>
public interface IPageManager : IDisposable
{
    /// <summary>
    /// Gets the total number of data pages across all files.
    /// </summary>
    int PageCount { get; }

    /// <summary>
    /// Allocates a new page.
    /// </summary>
    Page AllocatePage(PageType pageType = PageType.Data);

    /// <summary>
    /// Reads a page by ID.
    /// </summary>
    Page ReadPage(int pageId);

    /// <summary>
    /// Writes a page to storage.
    /// </summary>
    void WritePage(Page page);
}
