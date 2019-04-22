namespace LastModifiedQueryableExtensions.Utilities
{
    using System;
    using System.Collections.Async;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    internal static class TaskExtensions
    {
        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(
            this Task<IReadOnlyCollection<T>> source)
        {
            return new AsyncEnumerable<T>(
                async yield =>
                {
                    foreach (T item in await source)
                    {
                        await yield.ReturnAsync(item);
                    }
                });
        }

        public static IAsyncEnumerable<TOut> SelectAsync<TIn, TOut>(
            this IAsyncEnumerable<TIn> source,
            Func<TIn, Task<TOut>> projection)
        {
            return new AsyncEnumerable<TOut>(
                async yield =>
                {
                    var cancellationToken = yield.CancellationToken;
                    using (IAsyncEnumerator<TIn> asyncEnumerator = await source.GetAsyncEnumeratorAsync(cancellationToken))
                    {
                        while (await asyncEnumerator.MoveNextAsync(cancellationToken))
                        {
                            // TODO: Cancellation support in projection
                            TOut projected = await projection(asyncEnumerator.Current);
                            await yield.ReturnAsync(projected);
                        }
                    }
                });
        }

        public static async Task<TOut> MaxOrDefaultAsync<TIn, TOut>(
            this IAsyncEnumerable<TIn> source,
            Func<TIn, TOut> projection,
            TOut defaultValue = default(TOut),
            CancellationToken cancellationToken = default(CancellationToken))
            where TOut : IComparable<TOut>
        {
            TOut max = defaultValue;
            bool hasMax = false;
            using (IAsyncEnumerator<TIn> asyncEnumerator = await source.GetAsyncEnumeratorAsync(cancellationToken))
            {
                while (await asyncEnumerator.MoveNextAsync(cancellationToken))
                {
                    var projected = projection(asyncEnumerator.Current);
                    if (!hasMax)
                    {
                        max = projected;
                        hasMax = true;
                    }
                    else
                    {
                        if (projected.CompareTo(max) > 0)
                        {
                            max = projected;
                        }
                    }
                }
            }

            return max;
        }
    }
}