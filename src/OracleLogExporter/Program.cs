using System.Globalization;
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
                LastSuccessfulExportUtc = DateTime.UtcNow
            };

            await stateStore.SaveAsync(state);

            Console.WriteLine($"[{DateTime.UtcNow:O}] Exported {batch.Rows.Count} rows to {outputPath}. Watermark: {state.LastTimestampUtc:O} / {state.LastRowId}");
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
    public required string TimestampColumn { get; init; }
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
            TimestampColumn = NormalizeIdentifier(GetRequired("EXPORT_TIMESTAMP_COLUMN")),
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
}

sealed class TableMetadata
{
    public required string Owner { get; init; }
    public required string TableName { get; init; }
    public required string TimestampColumn { get; init; }
    public required IReadOnlyList<string> SelectedColumns { get; init; }

    public static async Task<TableMetadata> LoadAsync(OracleConnection connection, ExporterConfig config)
    {
        var owner = config.TableOwner ?? config.OracleUser.ToUpperInvariant();
        var allColumns = await ReadColumnsAsync(connection, owner, config.TableName);
        if (allColumns.Count == 0)
        {
            throw new InvalidOperationException($"Table {owner}.{config.TableName} not found or metadata not accessible.");
        }

        if (!allColumns.Contains(config.TimestampColumn, StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Timestamp column {config.TimestampColumn} not found in {owner}.{config.TableName}.");
        }

        var selected = allColumns
            .Where(c => !config.IgnoredColumns.Contains(c))
            .ToList();

        if (selected.Count == 0)
        {
            throw new InvalidOperationException("All columns were ignored. At least one column must be exported.");
        }

        Console.WriteLine($"[{DateTime.UtcNow:O}] Columns detected: {string.Join(", ", allColumns)}");
        if (config.IgnoredColumns.Count > 0)
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] Ignored columns: {string.Join(", ", config.IgnoredColumns)}");
        }

        return new TableMetadata
        {
            Owner = owner,
            TableName = config.TableName,
            TimestampColumn = config.TimestampColumn,
            SelectedColumns = selected
        };
    }

    private static async Task<List<string>> ReadColumnsAsync(OracleConnection connection, string owner, string table)
    {
        const string sql = """
            SELECT COLUMN_NAME
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :p_owner AND TABLE_NAME = :p_table
            ORDER BY COLUMN_ID
            """;

        await using var cmd = new OracleCommand(sql, connection);
        cmd.BindByName = true;
        cmd.Parameters.Add("p_owner", OracleDbType.Varchar2, owner, System.Data.ParameterDirection.Input);
        cmd.Parameters.Add("p_table", OracleDbType.Varchar2, table, System.Data.ParameterDirection.Input);

        var result = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(reader.GetString(0));
        }

        return result;
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
        var timestampSqlName = $"t.{QuoteIdentifier(table.TimestampColumn)}";

        var additionalFilter = string.IsNullOrWhiteSpace(config.AdditionalWhereClause)
            ? string.Empty
            : $" AND ({config.AdditionalWhereClause})";

        var sql = $"""
            SELECT *
            FROM (
                SELECT
                    ROWIDTOCHAR(t.ROWID) AS EXPORT_ROWID,
                    {selectColumns}
                FROM {qualifiedTable} t
                WHERE (
                    {timestampSqlName} > :lastTs
                    OR ({timestampSqlName} = :lastTs AND ROWIDTOCHAR(t.ROWID) > :lastRowId)
                )
                AND {timestampSqlName} <= :nowTs
                {additionalFilter}
                ORDER BY {timestampSqlName} ASC, ROWIDTOCHAR(t.ROWID) ASC
            )
            WHERE ROWNUM <= :batchSize
            """;

        await using var cmd = new OracleCommand(sql, connection);
        cmd.BindByName = true;
        cmd.Parameters.Add("lastTs", OracleDbType.TimeStamp, state.LastTimestampUtc, System.Data.ParameterDirection.Input);
        cmd.Parameters.Add("lastRowId", OracleDbType.Varchar2, state.LastRowId ?? string.Empty, System.Data.ParameterDirection.Input);
        cmd.Parameters.Add("nowTs", OracleDbType.TimeStamp, nowUtc, System.Data.ParameterDirection.Input);
        cmd.Parameters.Add("batchSize", OracleDbType.Int32, config.FetchBatchSize, System.Data.ParameterDirection.Input);

        var rows = new List<Dictionary<string, object?>>();
        DateTime lastTs = state.LastTimestampUtc;
        string? lastRowId = state.LastRowId;

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var rowId = reader.GetString(0);
            var timestampOrdinal = reader.GetOrdinal(table.TimestampColumn);
            var ts = reader.GetDateTime(timestampOrdinal);

            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["_row_id"] = rowId,
                ["_extracted_at_utc"] = DateTime.UtcNow
            };

            for (var i = 1; i < reader.FieldCount; i++)
            {
                var fieldName = reader.GetName(i);
                row[fieldName] = ConvertOracleValue(reader.GetValue(i));
            }

            rows.Add(row);
            lastTs = DateTime.SpecifyKind(ts, DateTimeKind.Utc);
            lastRowId = rowId;
        }

        return new ExportBatch
        {
            Rows = rows,
            LastTimestampUtc = lastTs,
            LastRowId = lastRowId
        };
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
    public DateTime LastSuccessfulExportUtc { get; set; }

    public static ExportState Initial(int lookbackMinutes)
    {
        return new ExportState
        {
            LastTimestampUtc = DateTime.UtcNow.AddMinutes(-lookbackMinutes),
            LastRowId = string.Empty,
            LastSuccessfulExportUtc = DateTime.MinValue
        };
    }
}

sealed class ExportBatch
{
    public required List<Dictionary<string, object?>> Rows { get; init; }
    public required DateTime LastTimestampUtc { get; init; }
    public required string? LastRowId { get; init; }
}
