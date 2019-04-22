namespace LastModifiedQueryableExtensions
{
    using System;
    using System.Collections.Async;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    using LastModifiedQueryableExtensions.Utilities;

    using StronglyTypedBlobStorage;

    public static class QueryableCachedByLastModifiedTimeExtensions
    {
        /// <summary>
        /// Wraps <paramref name="source"/> to query only the latest items NOT present in <paramref name="cache"/>
        /// using <paramref name="getLastModifiedTimeFromItem"/> to determine the items that have been updated.
        /// <para>
        /// Any items that the caller observes from the query will be observed to the cache as the caller observes them
        /// - Note this means if you query a bunch of items, and your IQueryable is eager loading them, but then you only
        /// enumerate a few, then the other items will NOT be added to cache and will be re-queried next time.
        /// - This is because it's possible for an <see cref="IQueryable{T}"/> to be never-ending, so we do not try
        /// to load all items returned into the cache unless the caller enumerates them.
        /// </para>
        /// </summary>
        /// <remarks>
        /// Becomes an IAsyncEnumerable to because we perform async operations on each MoveNext
        /// IAsyncQueryable would be appropriate as well, but does not exist
        /// </remarks>
        /// <param name="source">
        /// The queryable to wrap. WARNING - this must not have filtering that changes between usages of the same cache.
        /// Multiple calls to <see cref="QueryLatestFromRemoteAndObserveToCache{T}"/> with the same <paramref name="cache"/>
        /// must have the same <paramref name="source"/> to be proper usage.
        /// </param>
        /// <param name="cache">The location to cache the observed items.</param>
        /// <param name="getPersistentKey">
        /// A key that is used to persist items in <paramref name="cache"/>. WARNING - must not change during modifications
        /// </param>
        /// <param name="getLastModifiedTimeFromItem">How to get the last modified time for an item. Used for filtering and sorting.</param>
        /// <returns>
        /// An enumerable that will get the items from <paramref name="source"/> that are NOT in <paramref name="cache"/>, who's
        /// enumeration will add observed items to <paramref name="cache"/>
        /// </returns>
        public static IAsyncEnumerable<T> QueryLatestFromRemoteAndObserveToCache<T>(
            this IQueryable<T> source,
            IBlobStorage<T> cache,
            Func<T, string> getPersistentKey,
            Expression<Func<T, DateTimeOffset>> getLastModifiedTimeFromItem)
        {
            return new AsyncEnumerable<T>(
                async yield =>
                {
                    // Get the filter time from the existing items in the cache
                    DateTimeOffset latestUpdatedDate = await GetLatestUpdatedDateFromCache(cache, getLastModifiedTimeFromItem);

                    // Rewrite the source to filter by the filter time
                    source = source
                        .Where(ExpressionFactory.CreateIsAfterFilter(getLastModifiedTimeFromItem, latestUpdatedDate));

                    // Rewrite the source to order correctly
                    source = source
                        .OrderBy(getLastModifiedTimeFromItem);

                    foreach (var item in source)
                    {
                        string key = getPersistentKey(item);
                        await cache.WriteItemAsync(key, item);
                        await yield.ReturnAsync(item);
                    }
                });
        }

        private static async Task<DateTimeOffset> GetLatestUpdatedDateFromCache<T>(
            IBlobStorage<T> cache,
            Expression<Func<T, DateTimeOffset>> getLastModifiedTimeFromItem)
        {
            Func<T, DateTimeOffset> compile = getLastModifiedTimeFromItem.Compile();

            var latestUpdatedDate = await cache
                .ListAsync()
                .AsAsyncEnumerable()
                .SelectAsync(id => cache.ReadItemAsync(id.Id))
                .MaxOrDefaultAsync(item => compile(item), DateTimeOffset.MinValue);
            return latestUpdatedDate;
        }

        /// <summary>
        /// Wraps <paramref name="source"/> to query the latest versions of items, avoiding querying items in <paramref name="source"/>
        /// that have not been updated since they were added to <paramref name="cache"/>,
        /// using <paramref name="getLastModifiedTimeFromItem"/> to determine the items that have been updated.
        /// <para>
        /// Any items that the caller observes from the query will be observed to the cache as the caller observes them
        /// - Note this means if you query a bunch of items, and your IQueryable is eager loading them, but then you only
        /// enumerate a few, then the other items will NOT be added to cache and will be re-queried next time.
        /// - This is because it's possible for an <see cref="IQueryable{T}"/> to be never-ending, so we do not try
        /// to load all items returned into the cache unless the caller enumerates them.
        /// </para>
        /// WARNING - Items will NOT be returned in <paramref name="getLastModifiedTimeFromItem"/> order, to avoid returning
        /// stale information to the caller and to avoid eager loading of the <paramref name="source"/>. The ordering, while you should
        /// not depend on it, will be
        /// 1 Updated items in <paramref name="source"/> (in update order)
        /// 2 Items from <paramref name="cache"/> that are not newly updated (in unspecified order)
        /// </summary>
        /// <remarks>
        /// Becomes an IAsyncEnumerable to because we perform async operations on each MoveNext
        /// IAsyncQueryable would be appropriate as well, but does not exist
        /// </remarks>
        /// <param name="source">
        /// The queryable to wrap. WARNING - this must not have filtering that changes between usages of the same cache.
        /// Multiple calls to <see cref="QueryLatestFromRemoteAndObserveToCache{T}"/> with the same <paramref name="cache"/>
        /// must have the same <paramref name="source"/> to be proper usage.
        /// </param>
        /// <param name="cache">The location to cache the observed items.</param>
        /// <param name="getPersistentKey">
        /// A key that is used to persist items in <paramref name="cache"/>. WARNING - must not change during modifications
        /// </param>
        /// <param name="getLastModifiedTimeFromItem">How to get the last modified time for an item. Used for filtering and sorting.</param>
        /// <returns>
        /// An enumerable that will get the items from <paramref name="source"/> that are NOT in <paramref name="cache"/>, who's
        /// enumeration will add observed items to <paramref name="cache"/>
        /// </returns>
        public static IAsyncEnumerable<T> QueryLatestFromRemoteAndFromCache<T>(
            this IQueryable<T> source,
            IBlobStorage<T> cache,
            Func<T, string> getPersistentKey,
            Expression<Func<T, DateTimeOffset>> getLastModifiedTimeFromItem)
        {
            var remoteQuerySendsToCache = QueryLatestFromRemoteAndObserveToCache(
                source,
                cache,
                getPersistentKey,
                getLastModifiedTimeFromItem);

            return new AsyncEnumerable<T>(
                async yield =>
                {
                    // Figure out which items are in the cache that are not updated by this latest enumeration
                    var itemIdsInCacheBeforeUpdate = (await cache.ListAsync()).Select(id => id.Id);

                    // Enumerate the data, noting the items from cache that we will not double-return
                    var itemIdsToReturnFromCacheAfterEnumeration = new HashSet<string>(itemIdsInCacheBeforeUpdate);
                    using (IAsyncEnumerator<T> enumerator = await remoteQuerySendsToCache.GetAsyncEnumeratorAsync())
                    {
                        while (await enumerator.MoveNextAsync())
                        {
                            string key = getPersistentKey(enumerator.Current);
                            itemIdsToReturnFromCacheAfterEnumeration.Remove(key);
                            await yield.ReturnAsync(enumerator.Current);
                        }
                    }

                    // Then yield the old cached items that aren't newly updated
                    foreach (var id in itemIdsToReturnFromCacheAfterEnumeration)
                    {
                        T item = await cache.ReadItemAsync(id);
                        await yield.ReturnAsync(item);
                    }
                });
        }

        /// <summary>
        /// <see cref="QueryLatestFromRemoteAndFromCache{T}"/> but then will poll the <see cref="source"/> for new updates
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
        public static IAsyncEnumerable<T> QueryLatestFromRemoteAndFromCacheAndPoll<T>(
            this IQueryable<T> source,
            IBlobStorage<T> cache,
            Func<T, string> getPersistentKey,
            Expression<Func<T, DateTimeOffset>> getLastModifiedTimeFromItem,
            TimeSpan waitBetweenPolling)
        {
            var remoteQuerySendsToCache = QueryLatestFromRemoteAndObserveToCache(
                source,
                cache,
                getPersistentKey,
                getLastModifiedTimeFromItem);

            return new AsyncEnumerable<T>(
                async yield =>
                {
                    // Figure out which items are in the cache that are not updated by this latest enumeration
                    var itemIdsInCacheBeforeUpdate = (await cache.ListAsync()).Select(id => id.Id);

                    // Enumerate the data, noting the items from cache that we will not double-return
                    var itemIdsToReturnFromCacheAfterEnumeration = new HashSet<string>(itemIdsInCacheBeforeUpdate);
                    using (IAsyncEnumerator<T> enumerator = await remoteQuerySendsToCache.GetAsyncEnumeratorAsync())
                    {
                        while (await enumerator.MoveNextAsync())
                        {
                            string key = getPersistentKey(enumerator.Current);
                            itemIdsToReturnFromCacheAfterEnumeration.Remove(key);
                            await yield.ReturnAsync(enumerator.Current);
                        }
                    }

                    var waitBeforeNextPollTask = Task.Delay(waitBetweenPolling);

                    // Then yield the old cached items that aren't newly updated
                    foreach (var id in itemIdsToReturnFromCacheAfterEnumeration)
                    {
                        T item = await cache.ReadItemAsync(id);
                        await yield.ReturnAsync(item);
                    }

                    while (true)
                    {
                        await waitBeforeNextPollTask;

                        using (IAsyncEnumerator<T> enumerator = await remoteQuerySendsToCache.GetAsyncEnumeratorAsync())
                        {
                            waitBeforeNextPollTask = Task.Delay(waitBetweenPolling);
                            while (await enumerator.MoveNextAsync())
                            {
                                await yield.ReturnAsync(enumerator.Current);
                            }
                        }
                    }
                });
        }
    }
}