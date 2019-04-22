
namespace Tests
{
    using System;
    using System.Collections.Async;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using LastModifiedQueryableExtensions;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using Storage.Net;
    using Storage.Net.Blob;

    using StronglyTypedBlobStorage;

    [TestClass]
    public class CachedQueryableTests
    {
        private IList<(Guid id, DateTimeOffset timestamp)> sourceOfTruthData;
        private IBlobStorage<(Guid, DateTimeOffset)> cache;
        
        [TestInitialize]
        public void Setup()
        {
            this.sourceOfTruthData = new List<(Guid id, DateTimeOffset timestamp)>();
            for (int i = 0; i < 10; i++)
            {
                this.sourceOfTruthData.Add((Guid.NewGuid(), DateTimeOffset.Now));
                Thread.Sleep(1);
            }
            
            cache = BlobStorage<(Guid, DateTimeOffset)>.Create(
                StorageFactory.Blobs.InMemory(),
                new JsonSerializer<(Guid, DateTimeOffset)>());
        }

        [TestMethod]
        public async Task NothingInCache_GetsAllItems()
        {
            List<(Guid, DateTimeOffset)> results = await this.GetCachedQueryable().ToListAsync();

            Assert.AreEqual(this.sourceOfTruthData.Count, results.Count);
        }

        [TestMethod]
        public async Task ItemsInCacheObeyExpectedAgeOrdering_GetsAllItems()
        {
            var itemToCache = this.sourceOfTruthData.OrderBy(o => o.timestamp).First();
            await this.cache.WriteItemAsync(Guid.NewGuid().ToString(), itemToCache);

            List<(Guid, DateTimeOffset)> results = await this.GetCachedQueryable().ToListAsync();

            Assert.AreEqual(this.sourceOfTruthData.Count, results.Count);
        }

        [TestMethod]
        public async Task ItemsInCacheDISOBEYExpectedAgeOrdering_GetsOnlyLaterItems()
        {
            // *******************
            // NOTE - this should never happen, as long as we own the cache. If you modified the
            // cache and did something silly like this test does, we can't help you
            // -------------------
            // Why does this test exist then? Because it's easier to disobey the semantics
            // than to inspect which queries were run on an IQueryable and if they included the
            // DateTime filter correctly
            // *******************
            int itemsInSourceToSkip = 3;
            var itemToAddToCache = this.sourceOfTruthData
                .OrderBy(o => o.timestamp)
                .Skip(itemsInSourceToSkip)
                .First();

            await this.cache.WriteItemAsync(Guid.NewGuid().ToString(), itemToAddToCache);

            var enumeratedItems = await this.GetCachedQueryable().ToListAsync();

            // Should include the item added to the cache, but also everything after that
            Assert.AreEqual(
                this.sourceOfTruthData.Count - itemsInSourceToSkip,
                enumeratedItems.Count);
        }

        [TestMethod]
        public async Task IncludesItemsInCacheNotInSource()
        {
            // Simulating a previous run that got an item a long time ago
            var itemToAdd = (Guid.NewGuid(), DateTimeOffset.MinValue);
            await this.cache.WriteItemAsync(Guid.NewGuid().ToString(), itemToAdd);

            var enumeratedItems = await this.GetCachedQueryable().ToListAsync();

            Assert.AreEqual(1 + this.sourceOfTruthData.Count, enumeratedItems.Count);
            CollectionAssert.Contains(enumeratedItems, itemToAdd);
        }

        [TestMethod]
        public async Task ItemsAreObservedToCache()
        {
            Assert.AreEqual(0, (await this.cache.ListAsync()).Count());

            await this.GetCachedQueryable().ToListAsync();
            
            Assert.AreEqual(this.sourceOfTruthData.Count, (await this.cache.ListAsync()).Count());
        }

        [TestMethod]
        public async Task ItemsAreObservedToCache_InOrder()
        {
            int enumerationCount = 5;
            var expectedItems = this.sourceOfTruthData
                .OrderBy(o => o.timestamp)
                .Take(enumerationCount)
                .ToList();

            await this.GetCachedQueryable().Take(enumerationCount).ToListAsync();

            Assert.AreEqual(enumerationCount, (await this.cache.ListAsync()).Count());

            var itemsInCache = await Task.WhenAll(
                (await this.cache.ListAsync())
                .Select(id => this.cache.ReadItemAsync(id.Id)));

            CollectionAssert.AreEqual(itemsInCache, expectedItems);
        }

        [TestMethod]
        public async Task PolledVersionShouldNeverFinish()
        {
            var asyncEnumerable = this.sourceOfTruthData
                .AsQueryable()
                .QueryLatestFromRemoteAndFromCacheAndPoll(
                    this.cache,
                    tup => tup.Item1.ToString(),
                    tup => tup.Item2,
                    TimeSpan.FromSeconds(1));

            var toListTask = asyncEnumerable.ToListAsync();
            var considerNeverAfter = TimeSpan.FromSeconds(2);
            var considerNeverAfterTask = Task.Delay(considerNeverAfter);

            var firstFinished = await Task.WhenAny(considerNeverAfterTask, toListTask);
            if (firstFinished == toListTask)
            {
                Assert.Fail("ToList finished and should never finish");
            }
        }

        [TestMethod]
        public async Task PolledVersionShouldCatchUpdates()
        {
            var asyncEnumerable = this.sourceOfTruthData
                .AsQueryable()
                .QueryLatestFromRemoteAndFromCacheAndPoll(
                    this.cache,
                    tup => tup.Item1.ToString(),
                    tup => tup.Item2,
                    TimeSpan.FromSeconds(0.1));

            (Guid, DateTimeOffset) itemToAdd = (Guid.NewGuid(), DateTimeOffset.Now);

            var containsItemTask = asyncEnumerable.FirstAsync(item => item.Item1 == itemToAdd.Item1);

            // Should not finish until we add the item
            await Task.Delay(TimeSpan.FromSeconds(1));
            Assert.IsFalse(containsItemTask.IsCompleted);

            // Should finish (shortly) after we add the item
            this.sourceOfTruthData.Add(itemToAdd);
            await Task.Delay(TimeSpan.FromSeconds(0.5));
            Assert.IsTrue(containsItemTask.IsCompleted);
            Assert.IsFalse(containsItemTask.IsFaulted);

            // And it should be in the cache now
            IReadOnlyCollection<BlobId> allIds = await this.cache.ListAsync();
            Assert.AreEqual(this.sourceOfTruthData.Count, allIds.Count);
        }
        
        private IAsyncEnumerable<(Guid, DateTimeOffset)> GetCachedQueryable()
        {
            return this.sourceOfTruthData
                .AsQueryable()
                .QueryLatestFromRemoteAndFromCache(
                    this.cache,
                    tup => tup.Item1.ToString(),
                    tup => tup.Item2);
        }
    }
}