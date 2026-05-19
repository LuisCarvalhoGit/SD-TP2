FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
# Copiar as dependências partilhadas primeiro
COPY ["Gateway/Gateway.csproj", "Gateway/"]
COPY ["Shared/Shared.csproj", "Shared/"]
COPY ["Contracts/", "Contracts/"]
RUN dotnet restore "Gateway/Gateway.csproj"
# Copiar o resto do código e compilar
COPY . .
WORKDIR "/src/Gateway"
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Gateway.dll"]