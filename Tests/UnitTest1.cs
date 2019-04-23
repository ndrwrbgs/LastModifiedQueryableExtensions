
namespace Tests
{
    using System;
    using System.Collections.Async;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using LastModifiedQueryableExtensions;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using OpenTracing.Contrib.LocalTracers;
    using OpenTracing.Contrib.LocalTracers.Config.Console;
    using OpenTracing.Contrib.LocalTracers.Console;
    using OpenTracing.Mock;
    using OpenTracing.Util;

    using Storage.Net;
    using Storage.Net.Blob;

    using StronglyTypedBlobStorage;
    
    public class TracingConfigurationSection : ConfigurationSection
    {
        [ConfigurationProperty("console", IsRequired = true)]
        public ConsoleElement Console
        {
            get { return (ConsoleElement) base["console"]; }
        }
    }

    [TestClass]
    public class CachedQueryableTests
    {
        private struct TestItem
        {
            public Guid Id;
            public DateTimeOffset Timestamp;

            public TestItem(Guid id, DateTimeOffset timestamp)
            {
                this.Id = id;
                this.Timestamp = timestamp;
            }
        }

        private IList<TestItem> sourceOfTruthData;
        private IBlobStorage<TestItem> cache;
        
        [TestInitialize]
        public void Setup()
        {
            this.sourceOfTruthData = new List<TestItem>();
            for (int i = 0; i < 10; i++)
            {
                this.sourceOfTruthData.Add(new TestItem(Guid.NewGuid(), DateTimeOffset.Now));
                Thread.Sleep(1);
            }
            
            cache = BlobStorage<TestItem>.Create(
                StorageFactory.Blobs.InMemory(),
                new JsonSerializer<TestItem>());
        }

        [TestMethod]
        public async Task NothingInCache_GetsAllItems()
        {
            List<TestItem> results = await this.GetCachedQueryable().ToListAsync();

            Assert.AreEqual(this.sourceOfTruthData.Count, results.Count);
        }

        [TestMethod]
        public async Task ItemsInCacheObeyExpectedAgeOrdering_GetsAllItems()
        {
            var itemToCache = this.sourceOfTruthData.OrderBy(o => o.Timestamp).First();
            await this.cache.WriteItemAsync(Guid.NewGuid().ToString(), itemToCache);

            List<TestItem> results = await this.GetCachedQueryable().ToListAsync();

            Assert.AreEqual(this.sourceOfTruthData.Count, results.Count);
        }

        [TestMethod]
        public async Task DemoTracing()
        {
            GlobalTracer.Register(
                new MockTracer().Decorate(
                    ColoredConsoleTracerDecorationFactory.Create(
                        ((TracingConfigurationSection) ConfigurationManager.GetSection("tracing")).Console)));
            
            List<TestItem> results = await this.GetCachedQueryable().ToListAsync();
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
            // DateTimeOffset filter correctly
            // *******************
            int itemsInSourceToSkip = 3;
            var itemToAddToCache = this.sourceOfTruthData
                .OrderBy(o => o.Timestamp)
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
            var itemToAdd = new TestItem(Guid.NewGuid(), DateTimeOffset.MinValue);
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
            var expectedItems = this.sourceOfTruthData
                .OrderBy(o => o.Timestamp)
                .ToList();

            await this.GetCachedQueryable().ToListAsync();

            Assert.AreEqual(expectedItems.Count, (await this.cache.ListAsync()).Count());

            var itemsInCache = await Task.WhenAll(
                (await this.cache.ListAsync())
                .Select(id => this.cache.ReadItemAsync(id.Id)));

            CollectionAssert.AreEqual(itemsInCache, expectedItems);
        }

        [TestMethod]
        [Ignore /* We removed this because sorting wasn't possible in the service we are targeting */]
        public async Task ItemsAreObservedToCache_OnlyThoseEnumerated()
        {
            int enumerationCount = 5;
            var expectedItems = this.sourceOfTruthData
                .OrderBy(o => o.Timestamp)
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
                .QueryLatestFromRemoteAndFromCacheWithoutOrderByAndPoll(
                    this.cache,
                    tup => tup.Id.ToString(),
                    tup => tup.Timestamp,
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
                .QueryLatestFromRemoteAndFromCacheWithoutOrderByAndPoll(
                    this.cache,
                    tup => tup.Id.ToString(),
                    tup => tup.Timestamp,
                    TimeSpan.FromSeconds(0.1));

            TestItem itemToAdd = new TestItem(Guid.NewGuid(), DateTimeOffset.Now);

            var containsItemTask = asyncEnumerable.FirstAsync(item => item.Id == itemToAdd.Id);

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
        
        private IAsyncEnumerable<TestItem> GetCachedQueryable()
        {
            return this.sourceOfTruthData
                .AsQueryable()
                .QueryLatestFromRemoteAndFromCacheWithoutOrderBy(
                    this.cache,
                    tup => tup.Id.ToString(),
                    tup => tup.Timestamp);
        }
    }
}