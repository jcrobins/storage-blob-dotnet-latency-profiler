//------------------------------------------------------------------------------
//MIT License

//Copyright(c) 2018 Microsoft Corporation. All rights reserved.

//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:

//The above copyright notice and this permission notice shall be included in all
//copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//SOFTWARE.
//------------------------------------------------------------------------------

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Shared.Protocol;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;


namespace Sample_HighThroughputBlobUpload
{

    #region TestReporting
    class TestProgressReporter
    {
        public TestProgressReporter
            (uint reportFrequency,
             double percentile,
             ulong nEventsExpected)
        {
            ReportFrequency = reportFrequency;
            Percentile = percentile;
            NEventsExpected = nEventsExpected;
            KthElement = (ulong)(percentile / 100.0 * nEventsExpected);
            PercentileTrackingTree = new SortedSet<TimeSpan>();
            OperationSumInMs = 0.0;
            CurrentElement = 0;
        }

        public void ProcessTestEntry(object sender, TestDataEntryEventArgs e)
        {
            TestDataEntry testData = e.TestData;
            CurrentElement++;

            // Update percentile tracking.
            UpdatePercentileTracking(testData);
            // Update tracking for average time.
            OperationSumInMs += testData.ElapsedTime.TotalMilliseconds;

            if (CurrentElement % ReportFrequency == 0)
            {
                OutputUpdate();
            }
        }

        protected virtual void OutputUpdate()
        {
            Console.WriteLine($"Test {(100.0 * (CurrentElement / (double)NEventsExpected)).ToString("00.0")}% Complete.  " +
                $"{Percentile.ToString("00.###")}th Percentile: {(PercentileTrackingTree.Min.TotalMilliseconds).ToString("0.0")}ms  " +
                $"Average: {(OperationSumInMs / CurrentElement).ToString("0.0")}ms");
        }

        protected uint ReportFrequency { get; }
        protected double Percentile { get; }
        protected ulong NEventsExpected { get; }
        protected ulong KthElement { get; }
        protected SortedSet<TimeSpan> PercentileTrackingTree { get; }
        protected double OperationSumInMs { get; set; }
        protected ulong CurrentElement { get; set; }

        private void UpdatePercentileTracking(TestDataEntry testData)
        {
            if ((ulong)PercentileTrackingTree.Count < NEventsExpected - KthElement)
            {
                PercentileTrackingTree.Add(testData.ElapsedTime);
            }
            else if (PercentileTrackingTree.Min < testData.ElapsedTime)
            {
                PercentileTrackingTree.Remove(PercentileTrackingTree.Min);
                PercentileTrackingTree.Add(testData.ElapsedTime);
            }
        }
    }

    class TestDataEntryEventArgs : EventArgs
    {
        public TestDataEntryEventArgs(TestDataEntry testData)
        {
            TestData = testData;
        }
        public TestDataEntry TestData { get; }
    }

    class TestReporter
    {
        /// <summary>
        /// Fired whenever a test data entry is provided to the reporter.
        /// </summary>
        public event EventHandler<TestDataEntryEventArgs> EntryAddedEvent;

        public TestReporter()
        {
            TestData = new ConcurrentQueue<TestDataEntry>();
        }

        public void AddEntry(TestDataEntry testData)
        {
            TestData.Enqueue(testData);

            // Alert any who may want to keep track of live events.
            EntryAddedEvent(this, new TestDataEntryEventArgs(testData));
        }

        public void RecordStart()
        {
            TestStopwatch = new Stopwatch();
            TestStopwatch.Start();
        }

        public void RecordEnd()
        {
            if (TestStopwatch == null)
            {
                throw new InvalidOperationException("TestReporter.RecordStart() must be called prior to calling TestReporter.RecordEnd().");
            }

            TestStopwatch.Stop();
        }

        /// <summary>
        /// Writes all non-reported entries to the provided stream in the order they were added.
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public async Task ReportAllAsync(Stream stream, bool includeElapsedTime)
        {
            // Report elapsed time if requested and available.
            if (includeElapsedTime && TestStopwatch != null && !TestStopwatch.IsRunning)
            {
                StreamWriter writer = new StreamWriter(stream);
                await writer.WriteLineAsync($"Test Duration,{TestStopwatch.Elapsed.ToString("G")}");
                await writer.FlushAsync();
            }

            // Report the status of each individual 
            while (await ReportSingleAsync(stream)) ;
        }

        /// <summary>
        /// Writes the oldest non-reported entry to the provided stream.
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public async Task<bool> ReportSingleAsync(Stream stream)
        {
            bool hasData = TestData.TryDequeue(out TestDataEntry entry);

            // If there is data to report, report it.
            if (hasData)
            {
                StreamWriter writer = new StreamWriter(stream);
                await writer.WriteLineAsync(entry.Serialize());
                await writer.FlushAsync();
            }

            return hasData;
        }

        private ConcurrentQueue<TestDataEntry> TestData;
        private Stopwatch TestStopwatch;
    }
    class TestDataEntry
    {
        public TestDataEntry(TimeSpan elapsedTime)
        {
            ReportTime = DateTimeOffset.UtcNow;
            ElapsedTime = elapsedTime;
        }

        public DateTimeOffset ReportTime { get; }
        public TimeSpan ElapsedTime { get; }

        /// <summary>
        /// Serializes the object into a CSV format.
        /// </summary>
        /// <returns>A stringified version of this object in CSV format.</returns>
        public virtual string Serialize()
        {
            return $"{ReportTime},{ElapsedTime.TotalMilliseconds}";
        }
    }

    class DownloadTestDataEntry : TestDataEntry
    {
        public DownloadTestDataEntry
            (TimeSpan elapsedTime,
             string containerName,
             string blobName) : base(elapsedTime)
        {
            ContainerName = containerName;
            BlobName = blobName;
        }
        public string ContainerName { get; }
        public string BlobName { get; }
        public override string Serialize()
        {
            return $"{base.Serialize()},{ContainerName},{BlobName}";
        }
    }

    class UploadBlockTestDataEntry : TestDataEntry
    {
        public UploadBlockTestDataEntry
            (TimeSpan elapsedTime,
             string containerName,
             string blobName,
             string blockIndex) : base(elapsedTime)
        {
            ContainerName = containerName;
            BlobName = blobName;
        }
        public string ContainerName { get; }
        public string BlobName { get; }
        public string BlockId { get; }
        public override string Serialize()
        {
            return $"{base.Serialize()},{ContainerName},{BlobName},{BlockId}";
        }
    }

    class ListBlobTestDataEntry : TestDataEntry
    {
        public ListBlobTestDataEntry
            (TimeSpan elapsedTime,
             ulong nBlobsRetrieved) : base(elapsedTime)
        {
            NBlobsRetrieved = nBlobsRetrieved;
        }
        public ulong NBlobsRetrieved { get; }

        public override string Serialize()
        {
            return $"{base.Serialize()},{NBlobsRetrieved}";
        }
    }

    #endregion TestReporting
    abstract class Test
    {
        public Test
            (TestReporter reporter,
             CloudStorageAccount storageAccount,
             CloudBlobContainer container)
        {
            Reporter = reporter;
            StorageAccount = storageAccount;
            Container = container;
        }

        public virtual async Task Run()
        {
            Reporter.RecordStart();
            await RunInternal();
            Reporter.RecordEnd();
        }
        
        protected abstract Task RunInternal();

        protected TestReporter Reporter { get; }
        protected CloudStorageAccount StorageAccount { get; }
        protected CloudBlobContainer Container { get; }
    }
    class PutBlockTest : Test
    {
        public PutBlockTest
            (TestReporter testReporter,
             CloudStorageAccount storageAccount,
             CloudBlobContainer container,
             string[] testArgs) :
            base(testReporter, storageAccount, container)
        {
            if (!ParseArguments(testArgs, out string blobPrefix, out ulong nBlocksToUpload, out ulong blockSizeBytes, out uint? reportFrequency, out double? percentile))
            {
                throw new ArgumentException("The provided parameters were incorrect.");
            }

            BlobPrefix = blobPrefix;
            NBlocksToUpload = nBlocksToUpload;
            BlockSizeBytes = blockSizeBytes;

            if (reportFrequency != null && percentile != null)
            {
                TestProgressReporter progressReporter = new TestProgressReporter(reportFrequency.Value, percentile.Value, nBlocksToUpload);
                testReporter.EntryAddedEvent += progressReporter.ProcessTestEntry;
            }
        }

        protected override async Task RunInternal()
        {
            if (NBlocksToUpload == 0)
            {
                Console.WriteLine("Nothing to do.  NBlocksToUpload = 0.");
                return;
            }

            await Container.CreateIfNotExistsAsync();

            byte[] buffer = new byte[BlockSizeBytes];
            new Random().NextBytes(buffer);

            SemaphoreSlim sem = new SemaphoreSlim(5, 5);
            List<Task> tasks = new List<Task>();

            // Only use 100 blocks per blob.
            const ulong MAX_BLOCK_COUNT = 100;

            ulong nBlobsToUpload = NBlocksToUpload / MAX_BLOCK_COUNT + 1;

            for (ulong blobIndex = 0; blobIndex < nBlobsToUpload; ++blobIndex)
            {
                string blobName = BlobPrefix + blobIndex;
                CloudBlockBlob blob = Container.GetBlockBlobReference(blobName);

                ulong nBlocksToUpload = MAX_BLOCK_COUNT;
                if (blobIndex + 1 == nBlobsToUpload)
                {
                    nBlocksToUpload = NBlocksToUpload % MAX_BLOCK_COUNT;
                }
                for (ulong blockIndex = 0; blockIndex < nBlocksToUpload; ++blockIndex)
                {
                    string blockIdStr = Convert.ToBase64String(BitConverter.GetBytes(blockIndex));
                    Reporter.AddEntry(await UploadBlockAsync(buffer, blob, blockIdStr));
                }
            }
        }

        private string BlobPrefix { get; }
        private ulong NBlocksToUpload { get; }
        private ulong BlockSizeBytes { get; }


        private async Task<UploadBlockTestDataEntry> UploadBlockAsync(byte[] buffer, CloudBlockBlob cbb, string blockID)
        {
            MemoryStream ms = new MemoryStream(buffer);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            await cbb.PutBlockAsync(blockID, ms, "");
            stopwatch.Stop();

            return new UploadBlockTestDataEntry(stopwatch.Elapsed, cbb.Container.Name, cbb.Name, blockID);
        }
        private bool ParseArguments(string[] args, out string blobPrefix, out ulong nBlocksToUpload, out ulong blockSizeInBytes, out uint? reportFrequency, out double? percentile)
        {
            bool isValid = true;
            blobPrefix = "";
            nBlocksToUpload = 0;
            blockSizeInBytes = 0;
            reportFrequency = null;
            percentile = null;

            const ulong BLOCK_SIZE_LIMIT = 100 * 1024 * 1024;

            try
            {
                if (args.Length > 2)
                {
                    blobPrefix = args[0];
                    nBlocksToUpload = Convert.ToUInt64(args[1]);
                    blockSizeInBytes = Convert.ToUInt64(args[2]);

                    if (args.Length > 4)
                    {
                        reportFrequency = Convert.ToUInt32(args[3]);
                        percentile = Convert.ToDouble(args[4]);
                    }
                }
                else
                {
                    isValid = false;
                }

                // Validate fetched values.
                if (blockSizeInBytes > BLOCK_SIZE_LIMIT)
                {
                    Console.WriteLine($"BlockSizeInBytes cannot exceed the maximum allowed size for a PutBlock ({BLOCK_SIZE_LIMIT}).");
                    isValid = false;
                }
            }
            catch (Exception)
            {
                isValid = false;
            }

            if (!isValid)
            {
                Console.WriteLine("Invalid Arguments Provided to UploadTestBlocks.  Expected Arguments: [0]:blobPrefix [1]:nBlocksToUpload [2]:blockSizeInBytes [3]:reportFrequency [4]:percentile");
            }

            return isValid;
        }
    }

    class PutBlobTest : Test
    {
        public PutBlobTest
            (TestReporter testReporter,
             CloudStorageAccount storageAccount,
             CloudBlobContainer container,
             string[] testArgs) :
            base(testReporter, storageAccount, container)
        {
            if (!ParseArguments(testArgs, out string blobPrefix, out ulong nBlobsToUpload, out ulong blobSizeBytes, out uint? reportFrequency, out double? percentile))
            {
                throw new ArgumentException("The provided parameters were incorrect.");
            }

            BlobPrefix = blobPrefix;
            NBlobsToUpload = nBlobsToUpload;
            BlobSizeBytes = blobSizeBytes;

            if (reportFrequency != null && percentile != null)
            {
                TestProgressReporter progressReporter = new TestProgressReporter(reportFrequency.Value, percentile.Value, nBlobsToUpload);
                testReporter.EntryAddedEvent += progressReporter.ProcessTestEntry;
            }
        }

        protected override async Task RunInternal()
        {
            await Container.CreateIfNotExistsAsync();

            byte[] buffer = new byte[BlobSizeBytes];
            new Random().NextBytes(buffer);

            SemaphoreSlim sem = new SemaphoreSlim(5, 5);
            List<Task> tasks = new List<Task>();

            for (ulong i = 0; i < NBlobsToUpload; ++i)
            {
                string blobName = BlobPrefix + i;
                CloudBlockBlob blob = Container.GetBlockBlobReference(blobName);
                Reporter.AddEntry(await UploadBlobAsync(buffer, blob));
            }
        }

        private string BlobPrefix { get; }
        private ulong NBlobsToUpload { get; }
        private ulong BlobSizeBytes { get; }


        private async Task<DownloadTestDataEntry> UploadBlobAsync(byte[] buffer, CloudBlockBlob cbb)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            await cbb.UploadFromByteArrayAsync(buffer, 0, buffer.Length);
            stopwatch.Stop();

            return new DownloadTestDataEntry(stopwatch.Elapsed, cbb.Container.Name, cbb.Name);
        }
        private bool ParseArguments(string[] args, out string blobPrefix, out ulong nBlobsToUpload, out ulong blobSizeBytes, out uint? reportFrequency, out double? percentile)
        {
            bool isValid = true;
            blobPrefix = "";
            nBlobsToUpload = 0;
            blobSizeBytes = 0;
            reportFrequency = null;
            percentile = null;

            const ulong BLOB_SIZE_LIMIT = 256 * 1024 * 1024;

            try
            {
                if (args.Length > 2)
                {
                    blobPrefix = args[0];
                    nBlobsToUpload = Convert.ToUInt64(args[1]);
                    blobSizeBytes = Convert.ToUInt64(args[2]);

                    if (args.Length > 4)
                    {
                        reportFrequency = Convert.ToUInt32(args[3]);
                        percentile = Convert.ToDouble(args[4]);
                    }
                }
                else
                {
                    isValid = false;
                }

                // Validate fetched values.
                if (blobSizeBytes > BLOB_SIZE_LIMIT)
                {
                    Console.WriteLine($"BlobSizeBytes cannot exceed the maximum allowed size for a PutBlob ({BLOB_SIZE_LIMIT}).");
                    isValid = false;
                }
            }
            catch (Exception)
            {
                isValid = false;
            }

            if (!isValid)
            {
                Console.WriteLine("Invalid Arguments Provided to UploadTestBlobs.  Expected Arguments: [0]:blobPrefix [1]:nBlobsToUpload [2]:blobSizeBytes [3]:reportFrequency [4]:percentile");
            }

            return isValid;
        }
    }


    class UploadTestBlobs : Test
    {
        public UploadTestBlobs
            (TestReporter testReporter,
             CloudStorageAccount storageAccount,
             CloudBlobContainer container,
             string[] testArgs) :
            base(testReporter, storageAccount, container)
        {
            if (!ParseArguments(testArgs, out string blobPrefix, out ulong nBlobsToUpload, out ulong blobSizeBytes))
            {
                throw new ArgumentException("The provided parameters were incorrect.");
            }

            BlobPrefix = blobPrefix;
            NBlobsToUpload = nBlobsToUpload;
            BlobSizeBytes = blobSizeBytes;
        }

        protected override async Task RunInternal()
        {
            await Container.CreateIfNotExistsAsync();

            const ulong MAX_BLOCK_SIZE = 100 * 1024 * 1024;
            // Only generate enough real data to satisfy a single block.
            ulong uploadUnitBytes = Math.Min(BlobSizeBytes, MAX_BLOCK_SIZE);

            byte[] buffer = new byte[uploadUnitBytes];
            new Random().NextBytes(buffer);

            SemaphoreSlim sem = new SemaphoreSlim(100, 100);
            List<Task> tasks = new List<Task>();

            for (ulong i = 0; i < NBlobsToUpload; ++i)
            {
                string blobName = BlobPrefix + i;
                CloudBlockBlob blob = Container.GetBlockBlobReference(blobName);

                await sem.WaitAsync();
                tasks.Add(UploadBlobAsync(blob, buffer, BlobSizeBytes).ContinueWith((t) =>
                {
                    if (t.IsCompletedSuccessfully)
                    {
                        Console.WriteLine($"Blob {blobName} uploaded.");
                        sem.Release();
                    }
                    else
                    {
                        Console.WriteLine($"UploadBlobAsync for blob {blobName} failed.");
                        throw t.Exception;
                    }
                }));
            }

            // Wait for all blobs to successfully upload.
            await Task.WhenAll(tasks);
        }

        private string BlobPrefix { get; }
        private ulong NBlobsToUpload { get; }
        private ulong BlobSizeBytes { get; }

        private async Task UploadBlobAsync (CloudBlockBlob blob, byte[] buffer, ulong blobSizeBytes)
        {
            ulong blockSizeBytes = (ulong) buffer.Length;

            List<Task> tasks = new List<Task>();
            List<string> blockList = new List<string>();
            SemaphoreSlim semaphore = new SemaphoreSlim(5, 5);
            uint blockId;
            // Issue all PutBlocks for which 
            for (blockId = 0; blockId < blobSizeBytes / blockSizeBytes; ++blockId)
            {
                string blockIdStr = Convert.ToBase64String(BitConverter.GetBytes(blockId));
                MemoryStream ms = new MemoryStream(buffer);

                await semaphore.WaitAsync();

                tasks.Add(blob.PutBlockAsync(blockIdStr, ms, null).ContinueWith((t)=>
                {
                    if (t.IsCompletedSuccessfully)
                    {
                        blockList.Add(blockIdStr);
                        semaphore.Release();
                    }
                    else
                    {
                        Console.WriteLine($"PutBlock request for ID {blockIdStr} failed.");
                        throw t.Exception;
                    }
                }));
            }

            // Deal with the remaining bytes (last block containing fewer bytes than the others).
            ulong remainder = blobSizeBytes % blockSizeBytes;
            if (remainder > 0)
            {
                string blockIdStr = Convert.ToBase64String(BitConverter.GetBytes(blockId));
                MemoryStream ms = new MemoryStream(buffer, 0, (int)remainder);

                await semaphore.WaitAsync();

                tasks.Add(blob.PutBlockAsync(blockIdStr, ms, null).ContinueWith((t)=>
                {
                    if (t.IsCompletedSuccessfully)
                    {
                        blockList.Add(blockIdStr);
                        semaphore.Release();
                    }
                    else
                    {
                        Console.WriteLine($"PutBlock request for ID {blockIdStr} failed.");
                        throw t.Exception;
                    }
                }));
            }

            // Wait for all blocks to upload.
            await Task.WhenAll(tasks);

            // Commit the blob.
            await blob.PutBlockListAsync(blockList);
        }

        private bool ParseArguments(string[] args, out string blobPrefix, out ulong nBlobsToUpload, out ulong blobSizeBytes)
        {
            bool isValid = true;
            blobPrefix = "";
            nBlobsToUpload = 0;
            blobSizeBytes = 0;

            const ulong BLOCK_SIZE_LIMIT = 100 * 1024 * 1024;
            const uint BLOCK_COUNT_LIMIT = 50000;

            try
            {
                if (args.Length > 2)
                {
                    blobPrefix = args[0];
                    nBlobsToUpload = Convert.ToUInt64(args[1]);
                    blobSizeBytes = Convert.ToUInt64(args[2]);
                }
                else
                {
                    isValid = false;
                }

                // Validate fetched values.
                if (blobSizeBytes > BLOCK_SIZE_LIMIT * BLOCK_COUNT_LIMIT)
                {
                    Console.WriteLine($"BlobSizeBytes cannot exceed the maximum allowed size for a block blob ({BLOCK_SIZE_LIMIT * BLOCK_COUNT_LIMIT}).");
                    isValid = false;
                }
            }
            catch (Exception)
            {
                isValid = false;
            }

            if (!isValid)
            {
                Console.WriteLine("Invalid Arguments Provided to UploadTestBlobs.  Expected Arguments: [0]:blobPrefix [1]:nBlobsToUpload [2]:blobSizeBytes");
            }

            return isValid;
        }
    }

    class ListBlobsTest : Test
    {

        public ListBlobsTest
            (TestReporter testReporter,
             CloudStorageAccount storageAccount,
             CloudBlobContainer container,
             string[] args) :
            base(testReporter, storageAccount, container)
        {
            if (!ParseArguments(args, out string blobPrefix, out ulong nListOperations, out uint? reportFrequency, out double? percentile))
            {
                throw new ArgumentException("The provided parameters were incorrect.");
            }

            BlobPrefix = blobPrefix;
            NListOperations = nListOperations;

            if (reportFrequency != null && percentile != null)
            {
                TestProgressReporter progressReporter = new TestProgressReporter(reportFrequency.Value, percentile.Value, NListOperations);
                testReporter.EntryAddedEvent += progressReporter.ProcessTestEntry;
            }

            
        }

        protected override async Task RunInternal()
        {
            BlobContinuationToken continuationToken = null;
            Stopwatch stopwatch = new Stopwatch();
            do
            {
                stopwatch.Start();
                BlobResultSegment resultSegment = await Container.ListBlobsSegmentedAsync(BlobPrefix, continuationToken);
                stopwatch.Stop();

                // Count the number of items from the results.
                ulong nResults = 0;
                foreach (IListBlobItem item in resultSegment.Results)
                {
                    ++nResults;
                }

                continuationToken = resultSegment.ContinuationToken;
                Reporter.AddEntry(new ListBlobTestDataEntry(stopwatch.Elapsed, nResults));
                stopwatch.Reset();

            } while (continuationToken != null) ;
        }

        private string BlobPrefix { get; }
        private ulong NListOperations { get; }

        private bool ParseArguments
            (string[] args,
             out string blobPrefix,
             out ulong nListOperations,
             out uint? reportFrequency,
             out double? percentile)
        {
            bool isValid = true;
            blobPrefix = "";
            nListOperations = 0;
            reportFrequency = null;
            percentile = null;

            try
            {
                if (args.Length > 1)
                {
                    blobPrefix = args[0];
                    nListOperations = Convert.ToUInt64(args[1]);
                    if (args.Length > 3)
                    {
                        reportFrequency = Convert.ToUInt32(args[2]);
                        percentile = Convert.ToDouble(args[3]);
                    }
                }
                else
                {
                    isValid = false;
                }
            }
            catch (Exception)
            {
                isValid = false;
            }

            if (!isValid)
            {
                Console.WriteLine("Invalid Arguments Provided to ListBlobsTest.  Expected Arguments: [0]:blobPrefix [1]:nListOperations [2]:reportFrequency [3]:percentile");
            }

            return isValid;
        }
    }

    class RandomDownloadTest : Test
    {
        public RandomDownloadTest
            (TestReporter testReporter,
             CloudStorageAccount storageAccount,
             CloudBlobContainer container,
             string[] downloadTestArgs) :
            base(testReporter, storageAccount, container)
        {
            if (!ParseArguments(downloadTestArgs, out string blobPrefix, out ulong nBlobsToDownload, out long nBytesToDownloadPerBlob, out ulong? nBlobsAvailable, out uint? reportFrequency, out double? percentile))
            {
                throw new ArgumentException("The provided parameters were incorrect.");
            }
            BlobPrefix = blobPrefix;
            NBlobsToDownload = nBlobsToDownload;

            if (reportFrequency != null && percentile != null)
            {
                TestProgressReporter progressReporter = new TestProgressReporter(reportFrequency.Value, percentile.Value, NBlobsToDownload);
                testReporter.EntryAddedEvent += progressReporter.ProcessTestEntry;
            }

            if (nBlobsAvailable != null)
            {
                NBlobsAvailable = nBlobsAvailable.Value;
            }
            else
            {
                // Determine the number of blobs that exist in the container (not interested in tracking ListBlob perf here).
                NBlobsAvailable = GetNumBlobsInContainer().Result;
            }
            if (NBlobsAvailable > 0)
            {
                NBytesPerBlob = GetBytesPerBlob().Result;
                if (nBytesToDownloadPerBlob > NBytesPerBlob)
                {
                    throw new Exception($"Requested number of bytes to download per blob ({nBytesToDownloadPerBlob}) is greater than the size of blobs in container '{container.Name}' ({NBytesPerBlob}).");
                }
                NBytesPerBlob = nBytesToDownloadPerBlob;
            }
            else
            {
                throw new Exception($"No blobs exist in the specified container '{container.Name}'.");
            }
        }

        protected override async Task RunInternal()
        {
            // Initialize a buffer large enough to contain the contents of a single blob.
            byte[] buffer = new byte[NBytesPerBlob];

            // Choose the name of the first blob to retrieve.
            string nextBlobName = GetRandomBlobName();

            for (uint i = 1; i <= NBlobsToDownload; ++i)
            {
                MemoryStream readStream = new MemoryStream(buffer);

                var cbb = Container.GetBlockBlobReference(nextBlobName);

                Reporter.AddEntry(await DownloadBlobAsync(readStream, cbb));

                nextBlobName = GetRandomBlobName();
            }
        }

        private string BlobPrefix { get; }
        private ulong NBlobsToDownload { get; }
        private ulong NBlobsAvailable { get; }
        private long NBytesPerBlob { get; }

        private bool ParseArguments(string[] args, out string blobPrefix, out ulong nBlobsToDownload, out long nBytesToDownloadPerBlob, out ulong? nBlobsAvailable, out uint? reportFrequency, out double? percentile)
        {
            bool isValid = true;
            blobPrefix = "";
            nBlobsToDownload = 0;
            nBytesToDownloadPerBlob = 0;
            nBlobsAvailable = null;
            reportFrequency = null;
            percentile = null;

            try
            {
                if (args.Length > 2)
                {
                    blobPrefix = args[0];
                    nBlobsToDownload = Convert.ToUInt64(args[1]);
                    nBytesToDownloadPerBlob = Convert.ToInt64(args[2]);
                    if (args.Length > 4)
                    {
                        nBlobsAvailable = Convert.ToUInt64(args[3]);
                    }
                    if (args.Length > 5)
                    {
                        reportFrequency = Convert.ToUInt32(args[4]);
                        percentile = Convert.ToDouble(args[5]);
                    }
                }
                else
                {
                    isValid = false;
                }
            }
            catch (Exception)
            {
                isValid = false;
            }

            if (!isValid)
            {
                Console.WriteLine("Invalid Arguments Provided to RandomDownloadTest.  Expected Arguments: [0]:blobPrefix [1]:nBlobsToDownload [2]:nBytesToDownloadPerBlob [3]:nBlobsAvailable [4]:reportFrequency [5]:percentile");
            }

            return isValid;
        }

        private async Task<long> GetBytesPerBlob()
        {
            CloudBlockBlob sampleBlob = Container.GetBlockBlobReference(GetRandomBlobName());
            await sampleBlob.FetchAttributesAsync();
            return sampleBlob.Properties.Length;
        }

        private async Task<ulong> GetNumBlobsInContainer()
        {
            BlobContinuationToken continuationToken = null;
            ulong nMatchingBlobs = 0;
            while (true)
            {
                BlobResultSegment resultSegment = await Container.ListBlobsSegmentedAsync(BlobPrefix, continuationToken);
                if (resultSegment.ContinuationToken == null)
                {
                    break;
                }

                // Yay lazy evaluation
                foreach (IListBlobItem item in resultSegment.Results)
                {
                    ++nMatchingBlobs;
                }
                continuationToken = resultSegment.ContinuationToken;
            }
            return nMatchingBlobs;
        }

        /// <summary>
        /// Downloads the "NBytesPerBlob" of the given blob into the given stream and returns the amount of time taken.
        /// </summary>
        /// <param name="ms">Memory stream to populate with he </param>
        /// <param name="cbb"></param>
        /// <returns></returns>
        private async Task<DownloadTestDataEntry> DownloadBlobAsync(Stream stream, CloudBlockBlob cbb)
        {

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            await cbb.DownloadRangeToStreamAsync(stream, 0, NBytesPerBlob);
            stopwatch.Stop();

            return new DownloadTestDataEntry(stopwatch.Elapsed, cbb.Container.Name, cbb.Name);
        }

        /// <summary>
        /// Returns a random selection of a sequential list of "nBlobs" blobs named between "{blobPrefix}1" and "{blobPrefix}{nBlobs}".
        /// </summary>
        private string GetRandomBlobName()
        {
            int nBlobs = (int)NBlobsAvailable;
            if (NBlobsAvailable > Int32.MaxValue)
            {
                // 2 billion blobs is a bit excessive here.  Just move on with the Int32 cap instead of digging up a new rand implemenation :)
                Console.WriteLine($"Container {Container} has more blobs than currently handled by the test.  Restricting download range bettwen {BlobPrefix}1 and {BlobPrefix}{Int32.MaxValue}.");
                nBlobs = Int32.MaxValue;
            }
            return $"{BlobPrefix}{new Random().Next(0, (int)nBlobs-1)}";
        }
    }


    class Program
    {
        static CloudStorageAccount GetStorageAccount(
            string storageAccountNameEnvVar,
            string storageAccountSasEnvVar,
            string environment)
        {
            CloudStorageAccount storageAccount = null;

            string storageAccountName = Environment.GetEnvironmentVariable(storageAccountNameEnvVar);
            string storageAccountSas = Environment.GetEnvironmentVariable(storageAccountSasEnvVar);

            if (string.IsNullOrEmpty(storageAccountName))
            {
                throw new Exception($"Unable to connect to storage account. The environment variable '{storageAccountNameEnvVar}' is not defined.");
            }

            // If the provided storage account name is a connection string, parse it.
            if (storageAccountName.Contains("AccountName="))
            {
                storageAccount = CloudStorageAccount.Parse(storageAccountName);
            }
            // If a SAS wasn't provided, throw.
            else if(string.IsNullOrEmpty(storageAccountSas))
            {
                throw new Exception($"Unable to connect to storage account. The environment variable '{storageAccountSasEnvVar}' is not defined.");
            }
            // Use the SAS token.
            else
            {
                StorageCredentials credentials = new StorageCredentials(storageAccountSas);

                switch (environment.ToLower())
                {
                    case "prod":
                        storageAccount = new CloudStorageAccount(credentials, storageAccountName, null, true);
                        break;
                    case "preprod":
                        storageAccount = new CloudStorageAccount(credentials, storageAccountName, "preprod.core.windows.net", true);
                        break;
                    case "test":
                        storageAccount = new CloudStorageAccount(credentials, storageAccountName, "testch2new.dnsdemo4.com", true);
                        break;
                    default:
                        throw new Exception($"An unrecognized environment of '{environment}' was provided. Valid values are 'prod' and 'preprod'.");
                }
            }

            return storageAccount;
        }

        static void Main(string[] args)
        {
            // Parse base-level commmand line arguments.
            if (!ParseBaseArguments(args, out string containerName, out string storageAccountNameEnvVar, out string storageAccountSasEnvVar, out string outputFilePath, out string environment, out bool useHttps, out string testType, out string[] testArgs))
            {
                return;
            }

            try
            {
                CloudStorageAccount storageAccount = GetStorageAccount(storageAccountNameEnvVar, storageAccountSasEnvVar, environment);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = blobClient.GetContainerReference(containerName);

                // Create and Execute the Test.
                TestReporter reporter = new TestReporter();

                Test test = null;
                switch (testType.ToUpper())
                {
                    case "PUTBLOBTEST":
                        test = new PutBlobTest(reporter, storageAccount, container, testArgs);
                        break;
                    case "PUTBLOCKTEST":
                        test = new PutBlockTest(reporter, storageAccount, container, testArgs);
                        break;
                    case "RANDOMDOWNLOADTEST":
                        test = new RandomDownloadTest(reporter, storageAccount, container, testArgs);
                        break;
                    case "UPLOADTESTBLOBS":
                        test = new UploadTestBlobs(reporter, storageAccount, container, testArgs);
                        break;
                    case "LISTBLOBSTEST":
                        test = new ListBlobsTest(reporter, storageAccount, container, testArgs);
                        break;
                    default:
                        throw new NotImplementedException($"A test named {testType} has not been implemented.");
                }

                // Run the test on the managed thread pool.
                Task testTask = Task.Run(async () => { await test.Run(); });

                Console.WriteLine($"Test running.  Streaming details to output file '{outputFilePath}'.");

                // Flush the logs once per second to file until the test is complete.
                using (FileStream filestream = new FileStream(outputFilePath, FileMode.CreateNew, FileAccess.Write))
                {
                    // Periodically trigger a log flush (run on the thread pool) while we wait for the test to complete.
                    while (!testTask.IsCompleted)
                    {
                        Task.Run(async () =>
                        {
                            Task[] reportingTasks = new Task[]
                            {
                                reporter.ReportAllAsync(filestream, false),
                                Task.Delay(1000)
                            };

                            await Task.WhenAll(reportingTasks);
                        }).Wait();
                    }

                    if (testTask.IsFaulted)
                    {
                        throw testTask.Exception;
                    }

                    // One last flush to capture any final logs.
                    reporter.ReportAllAsync(filestream, false).Wait();
                }

                Console.WriteLine($"The test completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Test failed.  Details: {ex.Message}");
            }

        }


        static bool ParseBaseArguments(string[] args, out string containerName, out string storageAccountNameEnvVar, out string storageAccountSasEnvVar, out string outputFilePath, out string environment, out bool useHttps, out string testType, out string[] testSpecificArgs)
        {
            bool isValid = true;
            containerName = "";
            storageAccountNameEnvVar = "";
            storageAccountSasEnvVar = "";
            outputFilePath = "";
            environment = "";
            useHttps = true;
            testType = "";
            testSpecificArgs = null;

            try
            {
                if (args.Length > 5)
                {
                    containerName = args[0];
                    storageAccountNameEnvVar = args[1];
                    storageAccountSasEnvVar = storageAccountNameEnvVar + "Sas";
                    outputFilePath = args[2];
                    environment = args[3];
                    useHttps = Convert.ToBoolean(args[4]);
                    testType = args[5];

                    // Anything beyond this point is optional at this level and applies to the specific test type identified in arg[6].
                    if (args.Length > 6)
                    {
                        // Ugly, but couldn't find the right helper function :(.
                        testSpecificArgs = new string[args.Length - 6];
                        for(uint i = 6; i < args.Length; ++i)
                        {
                            testSpecificArgs[i - 6] = args[i];
                        }
                    }
                }
                else
                {
                    isValid = false;
                }
            }
            catch (Exception)
            {
                isValid = false;
            }

            if (!isValid)
            {
                Console.WriteLine("Invalid Arguments Provided.  Expected Arguments: arg0:containerName arg1:storageAccountNameEnvVar arg2:outputFilePath arg3:environment arg4:useHttps arg5:testType");
            }

            return isValid;
        }

    }

}
