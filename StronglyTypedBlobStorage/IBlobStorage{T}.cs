namespace StronglyTypedBlobStorage
{
    using System.Threading;
    using System.Threading.Tasks;

    using Storage.Net.Blob;

    public interface IBlobStorage<T> : IBlobStorage
    {
        Task WriteItemAsync(
            string id,
            T item,
            bool append = false,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<T> ReadItemAsync(
            string id,
            CancellationToken cancellationToken = default(CancellationToken));
    }
}