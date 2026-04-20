FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY src/OracleLogExporter/OracleLogExporter.csproj src/OracleLogExporter/
RUN dotnet restore src/OracleLogExporter/OracleLogExporter.csproj

COPY src/OracleLogExporter/ src/OracleLogExporter/
RUN dotnet publish src/OracleLogExporter/OracleLogExporter.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app

RUN mkdir -p /app/output /app/state
COPY --from=build /app/publish ./

ENTRYPOINT ["dotnet", "OracleLogExporter.dll"]
