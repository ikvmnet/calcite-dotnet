# Apache.Calcite.Adapter.Linq.Queryable

This infrastructure will be similar to the 'queryable' adapter in Apache Calcite, but for LINQ. It will allow us to execute LINQ queries against an IQueryable<T> data source, such as a database or an in-memory collection.

Instead of generating Janino code, we will generate Expression Trees that can be compiled and executed at runtime to create IQueryable<T> sequences. This will allow us to leverage the power of LINQ and the .NET runtime to execute queries efficiently.

