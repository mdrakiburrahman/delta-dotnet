using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Azure.Core;
using Azure.Identity;
using DeltaLake.Runtime;
using DeltaLake.Table;

namespace DeltaLake.Demo
{
    public class Program
    {
        private static readonly string _tempDir = Path.Combine(Directory.GetCurrentDirectory(), ".temp");
        private static readonly string _azureDir = "abfss://onelake@monitoringtestadls.dfs.core.windows.net/synapse/workspaces/monitoring-test-synapse/temp";
        private static readonly string _deltaTableTest = "demo-table";
        private static readonly string _testStringColumnName = "colStringTest";
        private static readonly string _testIntegerColumnName = "colIntegerTest";
        private static readonly string _testPartitionColumnName = "colAuthorIdTest";
        private static readonly int _numRows = 10;
        private static readonly int _numWriteLoops = 10;
        private static readonly string[] _storageScopes = new[] { "https://storage.azure.com/.default" };
        private static readonly int _numConcurrentWriters = 1;

        public static async Task Main(string[] args)
        {
            if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, true);
            _ = Directory.CreateDirectory(_tempDir);

            var adlsOauthToken = (await new VisualStudioCredential().GetTokenAsync(new TokenRequestContext(_storageScopes), default).ConfigureAwait(false)).Token;
            var testSchema = new Apache.Arrow.Schema.Builder()
                .Field(static fb => { fb.Name(_testStringColumnName);    fb.DataType(StringType.Default);   fb.Nullable(false); })
                .Field(static fb => { fb.Name(_testIntegerColumnName);   fb.DataType(Int32Type.Default);    fb.Nullable(false); })
                .Field(static fb => { fb.Name(_testPartitionColumnName); fb.DataType(Int32Type.Default);    fb.Nullable(false); })
                .Build();
            var runtime = CreateRuntime();
            var localPath = Path.Combine(_tempDir, _deltaTableTest);
            var adlsPath = $"{_azureDir}/{_deltaTableTest}";
            var localStorageOptions = new Dictionary<string, string> { };
            var adlsStorageOptions = new Dictionary<string, string> {{ "bearer_token", adlsOauthToken }};

            var localTable = await CreateDeltaTableAsync(runtime, localPath, testSchema, localStorageOptions, CancellationToken.None).ConfigureAwait(false);
            var azureTable = await CreateDeltaTableAsync(runtime, adlsPath,  testSchema, adlsStorageOptions,  CancellationToken.None).ConfigureAwait(false);

            Console.WriteLine($"Table {localTable.Location()}: {localTable.Version()}");
            Console.WriteLine($"Table {azureTable.Location()}: {azureTable.Version()}");

            // INSERT CONCURRENTLY
            //
            var tasks = new List<Task>();
            for (int i = 0; i < _numConcurrentWriters; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    for (int l = 0; l < _numWriteLoops; l++)
                    {
                        try
                        {
                            await InsertIntoTableAsync(runtime, localStorageOptions, testSchema, localPath, _numRows, i, CancellationToken.None).ConfigureAwait(false);
                            await InsertIntoTableAsync(runtime, adlsStorageOptions,  testSchema, adlsPath,  _numRows, i, CancellationToken.None).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                            throw;
                        }
                    }
                }));
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);

            // METADATA
            //
            Console.WriteLine($"Local partition columns: {string.Join(", ", localTable.Metadata().PartitionColumns)}");
            Console.WriteLine($"Local schema: {localTable.Metadata().SchemaString}");

            Console.WriteLine($"Azure partition columns: {string.Join(", ", azureTable.Metadata().PartitionColumns)}");
            Console.WriteLine($"Azure schema: {azureTable.Metadata().SchemaString}");

            runtime.Dispose();
        }

        private static DeltaRuntime CreateRuntime() => new(RuntimeOptions.Default);

        private static async Task<DeltaTable> CreateDeltaTableAsync(
            DeltaRuntime runtime,
            string path,
            Schema schema,
            Dictionary<string, string> storageOptions,
            CancellationToken cancellationToken
        ) =>
            await DeltaTable
                .CreateAsync(
                    runtime,
                    new TableCreateOptions(path, schema)
                    {
                        Configuration = new Dictionary<string, string?>(),
                        PartitionBy = new[] { _testPartitionColumnName },
                        StorageOptions = storageOptions,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

        private static async Task InsertIntoTableAsync(
            DeltaRuntime runtime,
            Dictionary<string, string> storageOptions,
            Schema schema,
            string location,
            int numRows,
            int authorId,
            CancellationToken cancellationToken
        )
        {
            using var table = await DeltaTable.LoadAsync(runtime, System.Text.Encoding.UTF8.GetBytes(location).AsMemory(), new TableOptions() { StorageOptions = storageOptions }, CancellationToken.None);
            Console.WriteLine($"Table {table.Location()}: {table.Version()}");

            var random = new Random();
            var allocator = new NativeMemoryAllocator();
            var recordBatchBuilder = new RecordBatch.Builder(allocator)
                .Append(
                    _testStringColumnName,
                    false,
                    col =>
                        col.String(arr =>
                            arr.AppendRange(
                                Enumerable
                                    .Range(0, numRows)
                                    .Select(_ => GenerateRandomString(random))
                            )
                        )
                )
                .Append(
                    _testIntegerColumnName,
                    false,
                    col =>
                        col.Int32(arr =>
                            arr.AppendRange(Enumerable.Range(0, numRows).Select(_ => random.Next()))
                        )
                )
                .Append(
                    _testPartitionColumnName,
                    false,
                    col =>
                        col.Int32(arr =>
                            arr.AppendRange(Enumerable.Range(0, numRows).Select(_ => authorId))
                        )
                );
            var options = new InsertOptions { SaveMode = SaveMode.Append };
            await table
                .InsertAsync(
                    new[] { recordBatchBuilder.Build() },
                    schema,
                    options,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        private static string GenerateRandomString(Random random, int length = 10)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            return new string(
                Enumerable.Repeat(chars, length).Select(s => s[random.Next(s.Length)]).ToArray()
            );
        }
    }
}
