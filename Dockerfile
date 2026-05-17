FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env
WORKDIR /app

COPY controlcenter.csproj ./
RUN dotnet restore controlcenter.csproj

COPY . ./
RUN dotnet publish controlcenter.csproj -c Release -o out

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app

COPY --from=build-env /app/out .

# 5000 = gRPC ingest from cross pods; 5001 = HTTP UI + REST + SSE.
EXPOSE 5000 5001
ENTRYPOINT [ "dotnet", "controlcenter.dll" ]
