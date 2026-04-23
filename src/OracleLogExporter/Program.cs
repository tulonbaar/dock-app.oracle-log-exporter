using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

var config = ExporterConfig.FromEnvironment();
Directory.CreateDirectory(config.OutputDirectory);
Directory.CreateDirectory(Path.GetDirectoryName(config.StateFilePath) ?? ".");

using var connection = new OracleConnection(config.ConnectionString);
await connection.OpenAsync();

var tableInfo = await TableMetadata.LoadAsync(connection, config);
var stateStore = new StateStore(config.StateFilePath);
var state = await stateStore.LoadAsync() ?? ExportState.Initial(config.InitialLookbackMinutes);

Console.WriteLine($"[{DateTime.UtcNow:O}] Exporter started for {tableInfo.Owner}.{tableInfo.TableName}. Poll interval: {config.PollIntervalSeconds}s");
Console.WriteLine($"[{DateTime.UtcNow:O}] Incremental mode: {tableInfo.DescribeIncrementalMode()}");

while (true)
{
    try
    {
        var nowUtc = DateTime.UtcNow;
        var batch = await OracleBatchReader.ReadBatchAsync(connection, tableInfo, state, nowUtc, config);

        if (batch.Rows.Count > 0)
        {
            var outputPath = JsonLogWriter.GetOutputPath(config.OutputDirectory, config.OutputFilePrefix, nowUtc);
            await JsonLogWriter.AppendAsync(outputPath, batch.Rows);

            state = new ExportState
            {
                LastTimestampUtc = batch.LastTimestampUtc,
                LastRowId = batch.LastRowId,
                LastTimestampFingerprints = batch.LastTimestampFingerprints,
                LastSuccessfulExportUtc = DateTime.UtcNow
            };

            await stateStore.SaveAsync(state);

            Console.WriteLine($"[{DateTime.UtcNow:O}] Exported {batch.Rows.Count} rows to {outputPath}. Watermark: {batch.WatermarkDescription}");
        }
        else
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] No new rows.");
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[{DateTime.UtcNow:O}] Export error: {ex.Message}");
    }

    await Task.Delay(TimeSpan.FromSeconds(config.PollIntervalSeconds));
}

sealed class ExporterConfig
{
    public required string OracleHost { get; init; }
    public int OraclePort { get; init; }
    public required string OracleServiceName { get; init; }
    public required string OracleUser { get; init; }
    public required string OraclePassword { get; init; }
    public required string TableName { get; init; }
    public string? TableOwner { get; init; }
    public string? TimestampColumn { get; init; }
    public HashSet<string> IgnoredColumns { get; init; } = new(StringComparer.OrdinalIgnoreCase);
    public string? AdditionalWhereClause { get; init; }
    public int PollIntervalSeconds { get; init; }
    public int FetchBatchSize { get; init; }
    public int InitialLookbackMinutes { get; init; }
    public required string OutputDirectory { get; init; }
    public required string OutputFilePrefix { get; init; }
    public required string StateFilePath { get; init; }

    public string ConnectionString =>
        $"User Id={OracleUser};Password={OraclePassword};Data Source={OracleHost}:{OraclePort}/{OracleServiceName};Pooling=true;";

    public static ExporterConfig FromEnvironment()
    {
        var tableSetting = GetRequired("EXPORT_TABLE");
        ParseTable(tableSetting, out var owner, out var tableName);

        return new ExporterConfig
        {
            OracleHost = GetRequired("ORACLE_HOST"),
            OraclePort = GetInt("ORACLE_PORT", 1521),
            OracleServiceName = GetRequired("ORACLE_SERVICE_NAME"),
            OracleUser = GetRequired("ORACLE_USER"),
            OraclePassword = GetRequired("ORACLE_PASSWORD"),
            TableName = tableName,
            TableOwner = owner,
            TimestampColumn = NormalizeOptionalIdentifier(Environment.GetEnvironmentVariable("EXPORT_TIMESTAMP_COLUMN")),
            IgnoredColumns = ParseCsvToSet(Environment.GetEnvironmentVariable("EXPORT_IGNORED_COLUMNS")),
            AdditionalWhereClause = Environment.GetEnvironmentVariable("EXPORT_ADDITIONAL_WHERE"),
            PollIntervalSeconds = GetInt("EXPORT_POLL_INTERVAL_SECONDS", 300),
            FetchBatchSize = GetInt("EXPORT_FETCH_BATCH_SIZE", 5000),
            InitialLookbackMinutes = GetInt("EXPORT_INITIAL_LOOKBACK_MINUTES", 5),
            OutputDirectory = Environment.GetEnvironmentVariable("EXPORT_OUTPUT_DIR") ?? "/app/output",
            OutputFilePrefix = Environment.GetEnvironmentVariable("EXPORT_OUTPUT_PREFIX") ?? "oracle-log",
            StateFilePath = Environment.GetEnvironmentVariable("EXPORT_STATE_FILE") ?? "/app/state/export-state.json"
        };
    }

    private static string GetRequired(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"Missing required environment variable: {name}");
        }

        return value.Trim();
    }

    private static int GetInt(string name, int defaultValue)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return defaultValue;
        }

        if (int.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
        {
            return parsed;
        }

        throw new InvalidOperationException($"Environment variable {name} must be a positive integer.");
    }

    private static HashSet<string> ParseCsvToSet(string? csv)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(csv))
        {
            return result;
        }

        foreach (var item in csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            result.Add(NormalizeIdentifier(item));
        }

        return result;
    }

    private static void ParseTable(string value, out string? owner, out string table)
    {
        var parts = value.Split('.', StringSplitOptions.TrimEntries);
        if (parts.Length == 1)
        {
            owner = null;
            table = NormalizeIdentifier(parts[0]);
            return;
        }

        if (parts.Length == 2)
        {
            owner = NormalizeIdentifier(parts[0]);
            table = NormalizeIdentifier(parts[1]);
            return;
        }

        throw new InvalidOperationException("EXPORT_TABLE should be TABLE or OWNER.TABLE.");
    }

    public static string NormalizeIdentifier(string identifier)
    {
        var trimmed = identifier.Trim();
        if (trimmed.Length >= 2 && trimmed.StartsWith('"') && trimmed.EndsWith('"'))
        {
            return trimmed[1..^1];
        }

        return trimmed.ToUpperInvariant();
    }

    private static string? NormalizeOptionalIdentifier(string? identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            return null;
        }

        return NormalizeIdentifier(identifier);
    }
}

enum IncrementalMode
{
    Timestamp,
    LatestBatch
}

sealed record ColumnDefinition(string Name, string DataType);

sealed class TableMetadata
{
    public required string Owner { get; init; }
    public required string TableName { get; init; }
    public string? TimestampColumn { get; init; }
    public required bool SupportsRowId { get; init; }
    public required IncrementalMode IncrementalMode { get; init; }
    public required IReadOnlyList<string> SelectedColumns { get; init; }

    public static async Task<TableMetadata> LoadAsync(OracleConnection connection, ExporterConfig config)
    {
        var owner = config.TableOwner ?? config.OracleUser.ToUpperInvariant();
        var allColumns = await ReadColumnsAsync(connection, owner, config.TableName);
        if (allColumns.Count == 0)
        {
            throw new InvalidOperationException($"Table {owner}.{config.TableName} not found or metadata not accessible.");
        }

        var columnNames = allColumns.Select(c => c.Name).ToList();
        var timestampColumn = ResolveTimestampColumn(config, allColumns, owner);
        var supportsRowId = await SupportsRowIdAsync(connection, owner, config.TableName);

        var selected = columnNames
            .Where(c => !config.IgnoredColumns.Contains(c))
            .ToList();

        if (selected.Count == 0)
        {
            throw new InvalidOperationException("All columns were ignored. At least one column must be exported.");
        }

        Console.WriteLine($"[{DateTime.UtcNow:O}] Columns detected: {string.Join(", ", columnNames)}");
        if (config.IgnoredColumns.Count > 0)
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] Ignored columns: {string.Join(", ", config.IgnoredColumns)}");
        }

        return new TableMetadata
        {
            Owner = owner,
            TableName = config.TableName,
            TimestampColumn = timestampColumn,
            SupportsRowId = supportsRowId,
            IncrementalMode = timestampColumn is null ? IncrementalMode.LatestBatch : IncrementalMode.Timestamp,
            SelectedColumns = selected
        };
    }

    public string DescribeIncrementalMode()
    {
        return IncrementalMode == IncrementalMode.Timestamp
            ? $"timestamp column {TimestampColumn}"
            : (SupportsRowId
                ? "latest batch fallback ordered by ROWID (no timestamp-compatible column detected)"
                : "latest batch fallback without stable ordering (no timestamp-compatible column detected)");
    }

    private static string? ResolveTimestampColumn(ExporterConfig config, IReadOnlyList<ColumnDefinition> columns, string owner)
    {
        ColumnDefinition? configuredColumn = null;
        if (config.TimestampColumn is not null)
        {
            configuredColumn = columns.FirstOrDefault(c => c.Name.Equals(config.TimestampColumn, StringComparison.OrdinalIgnoreCase));
            if (configuredColumn is null)
            {
                throw new InvalidOperationException($"Timestamp column {config.TimestampColumn} not found in {owner}.{config.TableName}.");
            }

            if (IsTimestampCompatible(configuredColumn.DataType))
            {
                Console.WriteLine($"[{DateTime.UtcNow:O}] Using configured timestamp column {configuredColumn.Name} ({configuredColumn.DataType}).");
                return configuredColumn.Name;
            }

            Console.WriteLine($"[{DateTime.UtcNow:O}] Configured timestamp column {configuredColumn.Name} has unsupported type {configuredColumn.DataType}. Trying auto-detection.");
        }

        var detectedColumn = columns.FirstOrDefault(c => IsTimestampCompatible(c.DataType));
        if (detectedColumn is not null)
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] Auto-detected timestamp column {detectedColumn.Name} ({detectedColumn.DataType}).");
            return detectedColumn.Name;
        }

        Console.WriteLine($"[{DateTime.UtcNow:O}] No timestamp-compatible column found in {owner}.{config.TableName}. Falling back to latest-batch mode.");
        return null;
    }

    private static async Task<bool> SupportsRowIdAsync(OracleConnection connection, string owner, string table)
    {
        var qualifiedTable = $"{QuoteIdentifier(owner)}.{QuoteIdentifier(table)}";
        var sql = $"SELECT ROWIDTOCHAR(t.ROWID) FROM {qualifiedTable} t WHERE ROWNUM = 1";

        try
        {
            await using var cmd = new OracleCommand(sql, connection);
            _ = await cmd.ExecuteScalarAsync();
            return true;
        }
        catch (OracleException)
        {
            return false;
        }
    }

    private static bool IsTimestampCompatible(string dataType)
    {
        return dataType.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            || dataType.Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || dataType.Equals("TIMESTAMP WITH TIME ZONE", StringComparison.OrdinalIgnoreCase)
            || dataType.Equals("TIMESTAMP WITH LOCAL TIME ZONE", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<List<ColumnDefinition>> ReadColumnsAsync(OracleConnection connection, string owner, string table)
    {
        const string sql = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :p_owner AND TABLE_NAME = :p_table
            ORDER BY COLUMN_ID
            """;

        await using var cmd = new OracleCommand(sql, connection);
        cmd.BindByName = true;
        cmd.Parameters.Add("p_owner", OracleDbType.Varchar2, owner, System.Data.ParameterDirection.Input);
        cmd.Parameters.Add("p_table", OracleDbType.Varchar2, table, System.Data.ParameterDirection.Input);

        var result = new List<ColumnDefinition>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(new ColumnDefinition(reader.GetString(0), reader.GetString(1)));
        }

        return result;
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}

sealed class OracleBatchReader
{
    public static async Task<ExportBatch> ReadBatchAsync(
        OracleConnection connection,
        TableMetadata table,
        ExportState state,
        DateTime nowUtc,
        ExporterConfig config)
    {
        var selectColumns = string.Join(", ", table.SelectedColumns.Select(c => $"t.{QuoteIdentifier(c)}"));
        var qualifiedTable = $"{QuoteIdentifier(table.Owner)}.{QuoteIdentifier(table.TableName)}";
        var timestampSqlName = table.TimestampColumn is null ? null : $"t.{QuoteIdentifier(table.TimestampColumn)}";

        var additionalFilter = string.IsNullOrWhiteSpace(config.AdditionalWhereClause)
            ? string.Empty
            : $" AND ({config.AdditionalWhereClause})";

        var sql = table.IncrementalMode == IncrementalMode.Timestamp
            ? $"""
                SELECT {selectColumns}
                FROM {qualifiedTable} t
                WHERE {timestampSqlName} >= :lastTs
                AND {timestampSqlName} <= :nowTs
                {additionalFilter}
                ORDER BY {timestampSqlName} ASC
                """
            : $"""
                SELECT {selectColumns}
                FROM (
                    SELECT {selectColumns}
                    FROM {qualifiedTable} t
                    WHERE 1 = 1
                    {additionalFilter}
                    {(table.SupportsRowId ? "ORDER BY t.ROWID DESC" : string.Empty)}
                )
                WHERE ROWNUM <= :batchSize
                """;

        await using var cmd = new OracleCommand(sql, connection);
        cmd.BindByName = true;
        if (table.IncrementalMode == IncrementalMode.Timestamp)
        {
            cmd.Parameters.Add("lastTs", OracleDbType.TimeStamp, state.LastTimestampUtc, System.Data.ParameterDirection.Input);
            cmd.Parameters.Add("nowTs", OracleDbType.TimeStamp, nowUtc, System.Data.ParameterDirection.Input);
        }
        else
        {
            cmd.Parameters.Add("batchSize", OracleDbType.Int32, config.FetchBatchSize, System.Data.ParameterDirection.Input);
        }

        var rows = new List<Dictionary<string, object?>>();
        DateTime lastTs = state.LastTimestampUtc;
        var lastTimestampFingerprints = new HashSet<string>(state.LastTimestampFingerprints ?? [], StringComparer.Ordinal);
        var currentBoundaryFingerprints = new HashSet<string>(lastTimestampFingerprints, StringComparer.Ordinal);
        var lastFingerprint = state.LastRowId;

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            DateTime? rowTimestamp = null;
            if (table.TimestampColumn is not null)
            {
                var timestampOrdinal = reader.GetOrdinal(table.TimestampColumn);
                var ts = reader.GetDateTime(timestampOrdinal);
                rowTimestamp = DateTime.SpecifyKind(ts, DateTimeKind.Utc);
            }

            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < reader.FieldCount; i++)
            {
                var fieldName = reader.GetName(i);
                row[fieldName] = ConvertOracleValue(reader.GetValue(i));
            }

            var fingerprint = ComputeRowFingerprint(table.SelectedColumns, row);

            if (table.IncrementalMode == IncrementalMode.Timestamp)
            {
                if (rowTimestamp is null)
                {
                    continue;
                }

                if (rowTimestamp.Value < state.LastTimestampUtc)
                {
                    continue;
                }

                if (rowTimestamp.Value == state.LastTimestampUtc && lastTimestampFingerprints.Contains(fingerprint))
                {
                    continue;
                }
            }

            row["_row_fingerprint"] = fingerprint;
            row["_extracted_at_utc"] = DateTime.UtcNow;

            rows.Add(row);

            if (table.IncrementalMode == IncrementalMode.Timestamp && rowTimestamp is not null)
            {
                if (rowTimestamp.Value > lastTs)
                {
                    lastTs = rowTimestamp.Value;
                    currentBoundaryFingerprints.Clear();
                }

                currentBoundaryFingerprints.Add(fingerprint);
            }

            lastFingerprint = fingerprint;

            if (rows.Count >= config.FetchBatchSize)
            {
                break;
            }
        }

        if (table.IncrementalMode == IncrementalMode.Timestamp && rows.Count == 0)
        {
            currentBoundaryFingerprints = new HashSet<string>(lastTimestampFingerprints, StringComparer.Ordinal);
        }

        return new ExportBatch
        {
            Rows = rows,
            LastTimestampUtc = lastTs,
            LastRowId = lastFingerprint,
            LastTimestampFingerprints = table.IncrementalMode == IncrementalMode.Timestamp
                ? currentBoundaryFingerprints.ToList()
                : [],
            WatermarkDescription = table.IncrementalMode == IncrementalMode.Timestamp
                ? $"{lastTs:O} / {currentBoundaryFingerprints.Count} fingerprints"
                : $"latest {rows.Count} rows"
        };
    }

    private static string ComputeRowFingerprint(IReadOnlyList<string> columnOrder, IReadOnlyDictionary<string, object?> row)
    {
        var builder = new StringBuilder();
        foreach (var column in columnOrder)
        {
            builder.Append(column);
            builder.Append('=');
            builder.Append(JsonSerializer.Serialize(row[column]));
            builder.Append('\u001f');
        }

        var bytes = Encoding.UTF8.GetBytes(builder.ToString());
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash);
    }

    private static object? ConvertOracleValue(object value)
    {
        if (value is DBNull)
        {
            return null;
        }

        return value switch
        {
            OracleDate oracleDate => oracleDate.Value,
            OracleTimeStamp oracleTimestamp => oracleTimestamp.Value,
            OracleTimeStampTZ oracleTimestampTz => oracleTimestampTz.Value,
            OracleTimeStampLTZ oracleTimestampLtz => oracleTimestampLtz.Value,
            OracleClob oracleClob => oracleClob.Value,
            OracleBlob oracleBlob => Convert.ToBase64String(oracleBlob.Value),
            DateTime dt => DateTime.SpecifyKind(dt, DateTimeKind.Utc),
            byte[] bytes => Convert.ToBase64String(bytes),
            _ => value
        };
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }
}

sealed class JsonLogWriter
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false
    };

    public static string GetOutputPath(string outputDir, string prefix, DateTime utcNow)
    {
        var fileName = $"{prefix}-{utcNow:yyyyMMdd}.jsonl";
        return Path.Combine(outputDir, fileName);
    }

    public static async Task AppendAsync(string outputPath, List<Dictionary<string, object?>> rows)
    {
        await using var stream = new FileStream(outputPath, FileMode.Append, FileAccess.Write, FileShare.Read);
        await using var writer = new StreamWriter(stream, Encoding.UTF8);

        foreach (var row in rows)
        {
            var json = JsonSerializer.Serialize(row, JsonOptions);
            await writer.WriteLineAsync(json);
        }
    }
}

sealed class StateStore
{
    private readonly string _stateFilePath;

    public StateStore(string stateFilePath)
    {
        _stateFilePath = stateFilePath;
    }

    public async Task<ExportState?> LoadAsync()
    {
        if (!File.Exists(_stateFilePath))
        {
            return null;
        }

        var json = await File.ReadAllTextAsync(_stateFilePath);
        return JsonSerializer.Deserialize<ExportState>(json);
    }

    public async Task SaveAsync(ExportState state)
    {
        var tmpPath = _stateFilePath + ".tmp";
        var json = JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(tmpPath, json);
        File.Move(tmpPath, _stateFilePath, overwrite: true);
    }
}

sealed class ExportState
{
    public DateTime LastTimestampUtc { get; set; }
    public string? LastRowId { get; set; }
    public List<string>? LastTimestampFingerprints { get; set; }
    public DateTime LastSuccessfulExportUtc { get; set; }

    public static ExportState Initial(int lookbackMinutes)
    {
        return new ExportState
        {
            LastTimestampUtc = DateTime.UtcNow.AddMinutes(-lookbackMinutes),
            LastRowId = string.Empty,
            LastTimestampFingerprints = [],
            LastSuccessfulExportUtc = DateTime.MinValue
        };
    }
}

sealed class ExportBatch
{
    public required List<Dictionary<string, object?>> Rows { get; init; }
    public required DateTime LastTimestampUtc { get; init; }
    public required string? LastRowId { get; init; }
    public required List<string> LastTimestampFingerprints { get; init; }
    public required string WatermarkDescription { get; init; }
}
