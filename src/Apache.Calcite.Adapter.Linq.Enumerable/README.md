# Apache.Calcite.Adapter.Linq.Enumerable

This will be an infrastructure very similar to the 'enumerable' adapter in Apache Calcite, but for LINQ. It will allow us to execute LINQ queries against an in-memory collection of data, and it will be used as a fallback when no other adapter can handle a query.

Instead of generating Janino code, we will generate Expression Trees that can be compiled and executed at runtime to create IEnumerable<T> sequences. This will allow us to leverage the power of LINQ and the .NET runtime to execute queries efficiently.

