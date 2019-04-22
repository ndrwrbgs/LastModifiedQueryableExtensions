namespace StronglyTypedBlobStorage {
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    using Storage.Net;
    using Storage.Net.Blob;

    public sealed class BlobStorage<T> : IBlobStorage<T>
    {
        private readonly IBlobStorage blobStorageImplementation;
        private readonly ISerializer<T> serializer;

        public static IBlobStorage<T> Create(IBlobStorage blobStorageImplementation, ISerializer<T> serializer)
        {
            return new BlobStorage<T>(blobStorageImplementation, serializer);
        }

        private BlobStorage(IBlobStorage blobStorageImplementation, ISerializer<T> serializer)
        {
            this.blobStorageImplementation = blobStorageImplementation;
            this.serializer = serializer;
        }

        async Task IBlobStorage<T>.WriteItemAsync(
            string id,
            T item,
            bool append,
            CancellationToken cancellationToken)
        {
            using (Stream stream = this.serializer.Serialize(item))
            {
                await this.blobStorageImplementation.WriteAsync(
                    id,
                    stream,
                    append,
                    cancellationToken);
            }
        }

        async Task<T> IBlobStorage<T>.ReadItemAsync(
            string id,
            CancellationToken cancellationToken)
        {
            using (var stream = await this.blobStorageImplementation.OpenReadAsync(id, cancellationToken))
            {
                return await this.serializer.Deserialize(stream);
            }
        }

        void IDisposable.Dispose()
        {
            this.blobStorageImplementation.Dispose();
        }

        Task<IReadOnlyCollection<BlobId>> IBlobStorage.ListAsync(ListOptions options, CancellationToken cancellationToken)
        {
            return this.blobStorageImplementation.ListAsync(options, cancellationToken);
        }

        Task IBlobStorage.WriteAsync(string id, Stream sourceStream, bool append, CancellationToken cancellationToken)
        {
            return this.blobStorageImplementation.WriteAsync(id, sourceStream, append, cancellationToken);
        }

        Task<Stream> IBlobStorage.OpenWriteAsync(string id, bool append, CancellationToken cancellationToken)
        {
            return this.blobStorageImplementation.OpenWriteAsync(id, append, cancellationToken);
        }

        Task<Stream> IBlobStorage.OpenReadAsync(string id, CancellationToken cancellationToken)
        {
            return this.blobStorageImplementation.OpenReadAsync(id, cancellationToken);
        }

        Task IBlobStorage.DeleteAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
        {
            return this.blobStorageImplementation.DeleteAsync(ids, cancellationToken);
        }

        Task<IReadOnlyCollection<bool>> IBlobStorage.ExistsAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
        {
            return this.blobStorageImplementation.ExistsAsync(ids, cancellationToken);
        }

        Task<IEnumerable<BlobMeta>> IBlobStorage.GetMetaAsync(IEnumerable<string> ids, CancellationToken cancellationToken)
        {
            return this.blobStorageImplementation.GetMetaAsync(ids, cancellationToken);
        }

        Task<ITransaction> IBlobStorage.OpenTransactionAsync()
        {
            return this.blobStorageImplementation.OpenTransactionAsync();
        }
    }
}