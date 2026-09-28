namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// The runtime state of a window frame for the row being evaluated, read by a window aggregate's
    /// generated code.
    /// </summary>
    /// <remarks>
    /// Holds the values Calcite's <c>WinAggFrameContext</c> describes at generation time: the row's index,
    /// the frame's bounds, whether it has rows, and the frame and partition row counts. Calcite's
    /// <c>EnumerableWindow</c> keeps them in local variables of its generated method;
    /// <c>ClrCursorDefaults.Window</c> runs the loop itself and passes this object to the generated lambdas,
    /// reusing one instance for every row.
    ///
    /// <para>Every index is into <see cref="Rows"/>, one partition sorted by the window's ordering.</para>
    /// </remarks>
    sealed class WindowFrame
    {

        /// <summary>
        /// Gets or sets the rows of the current partition, in the window's order.
        /// </summary>
        /// <remarks>
        /// An <c>object[]</c>, as in Calcite: a partition comes from <c>SortedMultiMap.arrays</c>, and every
        /// read converts the element to the row type.
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
        /// Gets or sets whether the frame holds any rows.
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
        /// Meaningful only while the adder runs. It is the <c>j</c> of Calcite's inner loop, and differs from
        /// <see cref="Index"/>, the row being evaluated.
        /// </remarks>
        public int Position { get; set; }

    }

}
