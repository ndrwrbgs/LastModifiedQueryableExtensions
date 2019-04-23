namespace LastModifiedQueryableExtensions
{
    using System;
    using System.Collections.Async;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    using LastModifiedQueryableExtensions.Utilities;

    using OpenTracing.Util;

    using StronglyTypedBlobStorage;

    public static partial class QueryableCachedByLastModifiedTimeExtensions
    {
        /// <summary>
        /// Wraps <paramref name="source"/> to query only the latest items NOT present in <paramref name="cache"/>
        /// using <paramref name="getLastModifiedTimeFromItem"/> to determine the items that have been updated.
        /// <para>
        /// ALL items returned from <paramref name="source"/> will be eagerly loaded and observed to <paramref name="cache"/>.
        /// This is to avoid problems with IQueryables that do not support OrderBy
        /// </para>
        /// </summary>
        /// <remarks>
        /// Becomes an IAsyncEnumerable to because we perform async operations on each MoveNext
        /// IAsyncQueryable would be appropriate as well, but does not exist
        /// </remarks>
        /// <param name="source">
        /// The queryable to wrap. WARNING - this must not have filtering that changes between usages of the same cache.
        /// Multiple calls to <see cref="QueryLatestFromRemoteAndObserveToCacheEagerly{T}"/> with the same <paramref name="cache"/>
        /// must have the same <paramref name="source"/> to be proper usage.
        /// </param>
        /// <param name="cache">The location to cache the observed items.</param>
        /// <param name="getPersistentKey">
        /// A key that is used to persist items in <paramref name="cache"/>. WARNING - must not change during modifications
        /// </param>
        /// <param name="getLastModifiedTimeFromItem">How to get the last modified time for an item. Used for filtering.</param>
        /// <returns>
        /// An enumerable that will get the items from <paramref name="source"/> that are NOT in <paramref name="cache"/>, who's
        /// enumeration will add observed items to <paramref name="cache"/>
        /// </returns>
        public static IAsyncEnumerable<T> QueryLatestFromRemoteAndObserveToCacheEagerly<T>(
            this IQueryable<T> source,
            IBlobStorage<T> cache,
            Func<T, string> getPersistentKey,
            Expression<Func<T, DateTimeOffset>> getLastModifiedTimeFromItem,
            IMetrics metrics = null)
        {
            metrics = metrics ?? NoopMetrics.Instance;

            return new AsyncEnumerable<T>(
                async yield =>
                {
                    using (GlobalTracer.Instance.BuildSpan($"{nameof(QueryLatestFromRemoteAndObserveToCacheEagerly)}.{nameof(AsyncEnumerator)}").StartActive())
                    {
                        // Get the filter time from the existing items in the cache
                        DateTimeOffset latestUpdatedDate = await GetLatestUpdatedDateFromCache(cache, getLastModifiedTimeFromItem, metrics);

                        // Rewrite the source to filter by the filter time
                        source = source
                            .Where(ExpressionFactory.CreateIsAfterFilter(getLastModifiedTimeFromItem, latestUpdatedDate));

                        // First make sure enumeration won't throw in the middle of it
                        List<T> allItemsInSource = GetAllItemsInRemote(source, metrics);

                        // Then observe all items to cache to ensure our lastModified watermark is correct
                        await ObserveItemsToCache(allItemsInSource, cache, getPersistentKey);

                        // Finally return items to the caller
                        foreach (T item in allItemsInSource)
                        {
                            await yield.ReturnAsync(item);
                        }
                    }
                });
        }

        private static async Task ObserveItemsToCache<T>(List<T> allItemsInSource, IBlobStorage<T> cache, Func<T, string> getPersistentKey)
        {
            using (GlobalTracer.Instance.BuildSpan(nameof(ObserveItemsToCache)).StartActive())
            {
                await Task.WhenAll(
                    allItemsInSource
                        .Select(item => cache.WriteItemAsync(getPersistentKey(item), item)));
            }
        }

        private static List<T> GetAllItemsInRemote<T>(IQueryable<T> source, IMetrics metrics)
        {
            using (GlobalTracer.Instance.BuildSpan(nameof(GetAllItemsInRemote)).StartActive())
            {
                List<T> allItemsInSource = source.ToList();

                metrics.RecordAllItemsInSourceCount(allItemsInSource.Count);
                return allItemsInSource;
            }
        }

        private static async Task<DateTimeOffset> GetLatestUpdatedDateFromCache<T>(
            IBlobStorage<T> cache,
            Expression<Func<T, DateTimeOffset>> getLastModifiedTimeFromItem,
            IMetrics metrics = null)
        {
            metrics = metrics ?? NoopMetrics.Instance;

            using (GlobalTracer.Instance.BuildSpan(nameof(GetLatestUpdatedDateFromCache)).StartActive())
            {
                Func<T, DateTimeOffset> compile = getLastModifiedTimeFromItem.Compile();

                var latestUpdatedDate = await cache
                    .ListAsync()
                    .AsAsyncEnumerable()
                    .SelectAsync(id => cache.ReadItemAsync(id.Id))
                    .Select(item => compile(item))
                    .MaxOrDefaultAsync(date => date, DateTimeOffset.MinValue);

                metrics.RecordLatestUpdatedDateFromCache(latestUpdatedDate);

                return latestUpdatedDate;
            }
        }

        /// <summary>
        /// Wraps <paramref name="source"/> to query the latest versions of items, avoiding querying items in <paramref name="source"/>
        /// that have not been updated since they were added to <paramref name="cache"/>,
        /// using <paramref name="getLastModifiedTimeFromItem"/> to determine the items that have been updated.
        /// <para>
        /// ALL items returned from <paramref name="source"/> will be eagerly loaded and observed to <paramref name="cache"/>.
        /// This is to avoid problems with IQueryables that do not support OrderBy
        /// </para>
        /// WARNING - Items will NOT be returned in <paramref name="getLastModifiedTimeFromItem"/> order, to avoid returning
        /// stale information to the caller and to avoid eager loading of the <paramref name="source"/>. The ordering, while you should
        /// not depend on it, will be
        /// 1 Updated items in <paramref name="source"/> (in unspecified order)
        /// 2 Items from <paramref name="cache"/> that are not newly updated (in unspecified order)
        /// </summary>
        /// <remarks>
        /// Becomes an IAsyncEnumerable to because we perform async operations on each MoveNext
        /// IAsyncQueryable would be appropriate as well, but does not exist
        /// </remarks>
        /// <param name="source">
        /// The queryable to wrap. WARNING - this must not have filtering that changes between usages of the same cache.
        /// Multiple calls to <see cref="QueryLatestFromRemoteAndObserveToCacheEagerly{T}"/> with the same <paramref name="cache"/>
        /// must have the same <paramref name="source"/> to be proper usage.
        /// </param>
        /// <param name="cache">The location to cache the observed items.</param>
        /// <param name="getPersistentKey">
        /// A key that is used to persist items in <paramref name="cache"/>. WARNING - must not change during modifications
        /// </param>
        /// <param name="getLastModifiedTimeFromItem">How to get the last modified time for an item. Used for filtering.</param>
        /// <returns>
        /// An enumerable that will get the items from <paramref name="source"/> that are NOT in <paramref name="cache"/>, who's
        /// enumeration will add observed items to <paramref name="cache"/>
        /// </returns>
        // TODO: Add a version that DOES support OrderBy to avoid eager loading
        public static IAsyncEnumerable<T> QueryLatestFromRemoteAndFromCacheWithoutOrderBy<T>(
            this IQueryable<T> source,
            IBlobStorage<T> cache,
            Func<T, string> getPersistentKey,
            Expression<Func<T, DateTimeOffset>> getLastModifiedTimeFromItem,
            IMetrics metrics = null)
        {
            metrics = metrics ?? NoopMetrics.Instance;

            var remoteQuerySendsToCache = QueryLatestFromRemoteAndObserveToCacheEagerly(
                source,
                cache,
                getPersistentKey,
                getLastModifiedTimeFromItem,
                metrics);

            return new AsyncEnumerable<T>(
                async yield =>
                {
                    using (GlobalTracer.Instance.BuildSpan($"{nameof(QueryLatestFromRemoteAndFromCacheWithoutOrderBy)}.{nameof(AsyncEnumerator)}").StartActive())
                    {
                        // Figure out which items are in the cache that are not updated by this latest enumeration
                        var itemIdsInCacheBeforeUpdate = await GetItemIdsFromCache(cache, metrics);

                        // Enumerate the data, noting the items from cache that we will not double-return
                        var itemIdsEnumeratedFromRemote = await EnumerateAndReturnFromRemote(getPersistentKey, remoteQuerySendsToCache, yield);
                        var itemIdsToReturnFromCacheAfterEnumeration = itemIdsInCacheBeforeUpdate.Except(itemIdsEnumeratedFromRemote);

                        // Then yield the old cached items that aren't newly updated
                        await EnumerateAndReturnFromCache(cache, itemIdsToReturnFromCacheAfterEnumeration, yield);
                    }
                });
        }

        private static async Task EnumerateAndReturnFromCache<T>(IBlobStorage<T> cache, IEnumerable<string> itemsToReturn, AsyncEnumerator<T>.Yield yield)
        {
            using (GlobalTracer.Instance.BuildSpan(nameof(EnumerateAndReturnFromCache)).StartActive())
            {
                foreach (var id in itemsToReturn)
                {
                    T item = await cache.ReadItemAsync(id);
                    await yield.ReturnAsync(item);
                }
            }
        }

        private static async Task<HashSet<string>> EnumerateAndReturnFromRemote<T>(
            Func<T, string> getPersistentKey,
            IAsyncEnumerable<T> remoteQuerySendsToCache,
            AsyncEnumerator<T>.Yield yield)
        {
            HashSet<string> itemKeysEnumeratedFromRemote = new HashSet<string>();
            using (GlobalTracer.Instance.BuildSpan(nameof(EnumerateAndReturnFromRemote)).StartActive())
            {
                using (IAsyncEnumerator<T> enumerator = await remoteQuerySendsToCache.GetAsyncEnumeratorAsync())
                {
                    while (await enumerator.MoveNextAsync())
                    {
                        string key = getPersistentKey(enumerator.Current);
                        itemKeysEnumeratedFromRemote.Add(key);
                        await yield.ReturnAsync(enumerator.Current);
                    }
                }
            }

            return itemKeysEnumeratedFromRemote;
        }

        private static async Task<IEnumerable<string>> GetItemIdsFromCache<T>(IBlobStorage<T> cache,
                                                                              IMetrics metrics = null)
        {
            metrics = metrics ?? NoopMetrics.Instance;

            using (GlobalTracer.Instance.BuildSpan(nameof(GetItemIdsFromCache)).StartActive())
                return (await cache.ListAsync()).Select(id => id.Id);
        }

        /// <summary>
        /// <see cref="QueryLatestFromRemoteAndFromCacheWithoutOrderBy{T}"/> but then will poll the <see cref="source"/> for new updates
        /// at the <see cref="waitBetweenPolling"/> interval.
        /// -
        /// Note that since it is polling, it will never terminate.
        /// </summary>
        /// <param name="waitBetweenPolling">
        /// The minimum time between subsequent <see cref="IEnumerable{T}.GetEnumerator"/> calls.
        /// NOTE - NOT the time between the enumerator finishing and calling the next time, we include the time that you spend iterating over the enumerator
        /// as counting towards this wait time.
        /// </param>
        /// <returns>A never-terminating enumerable</returns>
        public static IAsyncEnumerable<T> QueryLatestFromRemoteAndFromCacheWithoutOrderByAndPoll<T>(
            this IQueryable<T> source,
            IBlobStorage<T> cache,
            Func<T, string> getPersistentKey,
            Expression<Func<T, DateTimeOffset>> getLastModifiedTimeFromItem,
            TimeSpan waitBetweenPolling,
            IMetrics metrics = null)
        {
            metrics = metrics ?? NoopMetrics.Instance;

            var remoteQuerySendsToCache = QueryLatestFromRemoteAndObserveToCacheEagerly(
                source,
                cache,
                getPersistentKey,
                getLastModifiedTimeFromItem,
                metrics);

            return new AsyncEnumerable<T>(
                async yield =>
                {
                    using (GlobalTracer.Instance
                        .BuildSpan($"{nameof(QueryLatestFromRemoteAndFromCacheWithoutOrderByAndPoll)}.{nameof(AsyncEnumerator)}")
                        .WithTag($"{nameof(waitBetweenPolling)}.{nameof(TimeSpan.TotalMilliseconds)}", waitBetweenPolling.TotalMilliseconds)
                        .StartActive())
                    {
                        // Figure out which items are in the cache that are not updated by this latest enumeration
                        var itemIdsInCacheBeforeUpdate = await GetItemIdsFromCache(cache, metrics);

                        // Enumerate the data, noting the items from cache that we will not double-return
                        var itemIdsEnumeratedFromRemote = await EnumerateAndReturnFromRemote(getPersistentKey, remoteQuerySendsToCache, yield);
                        var itemIdsToReturnFromCacheAfterEnumeration = itemIdsInCacheBeforeUpdate.Except(itemIdsEnumeratedFromRemote);

                        var waitBeforeNextPollTask = Task.Delay(waitBetweenPolling);

                        // Then yield the old cached items that aren't newly updated
                        await EnumerateAndReturnFromCache(cache, itemIdsToReturnFromCacheAfterEnumeration, yield);

                        while (true)
                        {
                            using (GlobalTracer.Instance.BuildSpan("Wait between polling").StartActive())
                            {
                                await waitBeforeNextPollTask;
                            }

                            Task<HashSet<string>> enumerateAndReturnFromRemote = EnumerateAndReturnFromRemote(getPersistentKey, remoteQuerySendsToCache, yield);
                            waitBeforeNextPollTask = Task.Delay(waitBetweenPolling);
                            await enumerateAndReturnFromRemote;
                        }
                    }
                });
        }
    }

    public interface IMetrics
    {
        void RecordLatestUpdatedDateFromCache(DateTimeOffset latestUpdatedDate);
        void RecordAllItemsInSourceCount(int count);
    }

    public sealed class NoopMetrics : IMetrics
    {
        public static IMetrics Instance = new NoopMetrics();

        private NoopMetrics()
        {
            
        }

        public void RecordLatestUpdatedDateFromCache(DateTimeOffset latestUpdatedDate)
        {
            
        }

        public void RecordAllItemsInSourceCount(int count)
        {
            
        }
    }
}