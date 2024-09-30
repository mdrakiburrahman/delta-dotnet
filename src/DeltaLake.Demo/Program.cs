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
        private static readonly string _tempDir = Path.Combine(
            Directory.GetCurrentDirectory(),
            ".temp"
        );
        private static readonly string _azureDir = "abfss://onelake@monitoringtestadls.dfs.core.windows.net/synapse/workspaces/monitoring-test-synapse/temp";
        private static readonly string _deltaTableTest = "demo-table";
        private static readonly string _testStringColumnName = "colStringTest";
        private static readonly string _testIntegerColumnName = "colIntegerTest";
        private static readonly int _numRows = 10;
        private static readonly string[] _storageScopes = new[] { "https://storage.azure.com/.default" };

        public static async Task Main(string[] args)
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, true);
            _ = Directory.CreateDirectory(_tempDir);

            var adlsOauthToken = (
                await new VisualStudioCredential().GetTokenAsync(
                    new TokenRequestContext(_storageScopes),
                    default
                ).ConfigureAwait(false)
            ).Token;

            var testSchema = new Apache.Arrow.Schema.Builder()
                .Field(static fb =>
                {
                    fb.Name(_testStringColumnName);
                    fb.DataType(StringType.Default);
                    fb.Nullable(false);
                })
                .Field(static fb =>
                {
                    fb.Name(_testIntegerColumnName);
                    fb.DataType(Int32Type.Default);
                    fb.Nullable(false);
                })
                .Build();

            var runtime = CreateRuntime();
            var localTable = await CreateDeltaTableAsync(
                    runtime,
                    Path.Combine(_tempDir, _deltaTableTest),
                    testSchema,
                    CancellationToken.None
                )
                .ConfigureAwait(false);
            var azureTable = await CreateDeltaTableAdlsAsync(
                    runtime,
                    $"{_azureDir}/{_deltaTableTest}",
                    adlsOauthToken,
                    testSchema,
                    CancellationToken.None
                )
                .ConfigureAwait(false);

            // INSERT
            //
            await InsertIntoTableAsync(localTable, testSchema, _numRows, CancellationToken.None)
                .ConfigureAwait(false);
            await InsertIntoTableAsync(azureTable, testSchema, _numRows, CancellationToken.None)
                .ConfigureAwait(false);

            runtime.Dispose();
        }

        private static DeltaRuntime CreateRuntime() => new(RuntimeOptions.Default);

        private static async Task<DeltaTable> CreateDeltaTableAsync(
            DeltaRuntime runtime,
            string path,
            Schema schema,
            CancellationToken cancellationToken
        ) =>
            await DeltaTable
                .CreateAsync(
                    runtime,
                    new TableCreateOptions(path, schema)
                    {
                        Configuration = new Dictionary<string, string?>(),
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

        private static async Task<DeltaTable> CreateDeltaTableAdlsAsync(
            DeltaRuntime runtime,
            string path,
            string bearerToken,
            Schema schema,
            CancellationToken cancellationToken
        ) =>
            await DeltaTable
                .CreateAsync(
                    runtime,
                    new TableCreateOptions(path, schema)
                    {
                        Configuration = new Dictionary<string, string?>(),
                        StorageOptions = new Dictionary<string, string?>
                        {
                            { "bearer_token", bearerToken },
                        },
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

        private static async Task InsertIntoTableAsync(
            DeltaTable table,
            Schema schema,
            int numRows,
            CancellationToken cancellationToken
        )
        {
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
