# Apache.Calcite.Adapter.AdoNet

This project implements a Calcite Adapter for ADO.NET. This should allow mapping `AdoSchema`s through Calcite, and pushing down operations. This is very early work.

# Apache.Calcite.Data

The goal of this project is to produce an ADO.NET Connection for Apache Calcite that mostly mimics the Avatica JDBC driver. `ikvm-jdbc` implements a ADO.NET -> JDBC wrapper. It is possible much of the code in there can be reused.

This is currently non-functional. Calcite needs to be accessed either by hand or through JDBC presently.
