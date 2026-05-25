# Apache.Calcite.Adapter.AdoNet

`Apache.Calcite.Adapter.AdoNet` bridges Calcite's optimizer/runtime with external ADO.NET data sources.

It provides adapter primitives such as:

- `AdoSchema` / `AdoDatabaseSchema` for exposing ADO.NET-backed sources as Calcite schemas
- relational conversion rules and implementations for pushdown (`AdoFilter`, `AdoProject`, `AdoJoin`, etc.)
- metadata abstractions for provider-specific behavior
- data-source integration helpers (`AdoDataSource`, `DbProviderAdoDataSource`, `DbDataSourceAdoDataSource`)

## Install

```sh
dotnet add package Apache.Calcite.Adapter.AdoNet
```

## When to use this package

Use this package when you want Calcite to query other databases/providers through ADO.NET and push supported
operations down to those sources.

## License

Apache License 2.0.
