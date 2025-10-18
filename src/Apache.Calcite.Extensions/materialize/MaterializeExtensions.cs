namespace org.apache.calcite.materialize;

public static class MaterializeExtensions
{

    /// <summary>
    /// Returns a <see cref="LatticeRef"/> wrapping the specified <see cref="Lattice"/>.
    /// </summary>
    /// <param name="self"></param>
    /// <returns></returns>
    public static LatticeRef AsRef(this Lattice? self) => LatticeRef.Create(self);

}
