namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// What the pieces of a window aggregate read about the row they are being evaluated for.
    /// </summary>
    /// <remarks>
    /// The values <c>WinAggFrameContext</c> describes -- index, startIndex, endIndex, hasRows and the two row
    /// counts -- held rather than described. That interface is Calcite's at generation time, its members
    /// answering with an <c>Expression</c> standing for each; there is no runtime counterpart to mirror,
    /// because Janino writes them into the generated method as plain locals.
    ///
    /// <para>Calcite's <c>EnumerableWindow</c> declares these as local variables of the generated method and the
    /// aggregate implementors write expressions naming them. <see cref="ClrEnumerableDefaults.Window"/> owns the loop
    /// instead, so they are values it hands to the lambdas; one instance is reused for every row, exactly as
    /// one set of variables is.
    ///
    /// <para>Every index is into <see cref="Rows"/>, which is one partition sorted by the window's ordering.
    /// A start of -1 means the frame is empty, which is what <see cref="HasRows"/> says.</para>
    /// </remarks>
    sealed class WindowFrame
    {

        /// <summary>
        /// Gets or sets the rows of the current partition, in the window's order.
        /// </summary>
        /// <remarks>
        /// An object array, because Calcite's is: a partition comes out of <c>SortedMultiMap.arrays</c> and
        /// every read of it converts to the row type.
        /// </remarks>
        public object[] Rows { get; set; } = [];

        /// <summary>
        /// Gets or sets the index of the row being evaluated.
        /// </summary>
        public int Index { get; set; }

        /// <summary>
        /// Gets or sets the index of the first row of the frame, or -1 where the frame is empty.
        /// </summary>
        public int Start { get; set; }

        /// <summary>
        /// Gets or sets the index of the last row of the frame, or -1 where the frame is empty.
        /// </summary>
        public int End { get; set; }

        /// <summary>
        /// Gets or sets whether the frame holds any row at all.
        /// </summary>
        public bool HasRows { get; set; }

        /// <summary>
        /// Gets or sets how many rows the frame holds.
        /// </summary>
        public int FrameRowCount { get; set; }

        /// <summary>
        /// Gets or sets how many rows the partition holds.
        /// </summary>
        public int PartitionRowCount { get; set; }

        /// <summary>
        /// Gets or sets the index of the row being folded into the accumulator.
        /// </summary>
        /// <remarks>
        /// Only meaningful while the adder runs. It is the <c>j</c> of Calcite's inner loop, which is not the
        /// row being evaluated: that is <see cref="Index"/>.
        /// </remarks>
        public int Position { get; set; }

    }

}
