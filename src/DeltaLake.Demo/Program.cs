using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using DeltaLake.Runtime;
using DeltaLake.Table;

namespace DeltaLake.Demo
{
    public class Program
    {
        private static readonly string _tempDir = Path.Combine(Directory.GetCurrentDirectory(), ".temp");
        private static readonly string _deltaTableTest = "myDeltaTable";
        private static readonly string _testColumnName = "colTest";
        private static readonly int _numRows = 10;

        public static async Task Main(string[] args)
        {
            if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, true);
            _ = Directory.CreateDirectory(_tempDir);
            var testSchema = new Apache.Arrow.Schema.Builder()
                .Field(static fb =>
                {
                    fb.Name(_testColumnName);
                    fb.DataType(StringType.Default);
                    fb.Nullable(false);
                })
                .Build();
            var runtime = CreateRuntime();
            var table = await CreateDeltaTableAsync(
                    runtime,
                    Path.Combine(_tempDir, _deltaTableTest),
                    testSchema,
                    CancellationToken.None
                )
                .ConfigureAwait(false);

            // INSERT
            //
            await InsertIntoTableAsync(table, testSchema, _numRows, CancellationToken.None).ConfigureAwait(false);

            runtime.Dispose();
        }

        private static DeltaRuntime CreateRuntime() => new(RuntimeOptions.Default);

        private static async Task<DeltaTable> CreateDeltaTableAsync(
            DeltaRuntime runtime,
            string path,
            Schema schema,
            CancellationToken cancellationToken
        ) => await DeltaTable
                .CreateAsync(
                    runtime,
                    new TableCreateOptions(path, schema)
                    {
                        Configuration = new Dictionary<string, string?>(),
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
            var recordBatchBuilder = new RecordBatch.Builder(allocator).Append(
                _testColumnName,
                false,
                col =>
                    col.String(arr =>
                        arr.AppendRange(
                            Enumerable.Range(0, numRows).Select(_ => GenerateRandomString(random))
                        )
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
