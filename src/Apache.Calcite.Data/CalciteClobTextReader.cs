using System;
using System.IO;

using java.sql;

namespace Apache.Calcite.Data
{

    class CalciteClobTextReader : TextReader
    {

        readonly Clob _clob;
        CalciteTextReader? _reader;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="clob"></param>
        public CalciteClobTextReader(Clob clob)
        {
            _clob = clob ?? throw new ArgumentNullException(nameof(clob));
        }

        /// <summary>
        /// Initializes and retrieves the <see cref="CalciteTextReader"/>.
        /// </summary>
        CalciteTextReader Reader => _reader ??= new CalciteTextReader(_clob.getCharacterStream());

        /// <inheritdoc />
        public override int Peek() => Reader.Peek();

        /// <inheritdoc />
        public override int Read() => Reader.Read();

        /// <inheritdoc />
        public override int Read(char[] buffer, int index, int count) => Reader.Read(buffer, index, count);

#if NET

        /// <inheritdoc />
        public override int Read(Span<char> buffer) => Reader.Read(buffer);

#endif

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _reader?.Dispose();
                _clob.free();
            }

            base.Dispose(disposing);
        }

    }

}
