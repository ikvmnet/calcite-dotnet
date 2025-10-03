using System;
using System.Buffers;
using System.IO;

using java.io;

namespace Apache.Calcite.Data
{

    class CalciteTextReader : TextReader
    {

        readonly Reader _input;
        PushbackReader? _reader;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="input"></param>
        public CalciteTextReader(Reader input)
        {
            _input = input ?? throw new ArgumentNullException(nameof(input));
        }

        /// <summary>
        /// Initializes and retrieves the <see cref="PushbackReader"/>.
        /// </summary>
        PushbackReader Reader => _reader ??= new PushbackReader(_input, 1);

        /// <inheritdoc />
        public override int Peek()
        {
            try
            {
                var r = Reader;
                var c = r.read();
                r.unread(c);
                return c;
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <inheritdoc />
        public override int Read()
        {
            try
            {
                return Reader.read();
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <inheritdoc />
        public override int Read(char[] buffer, int index, int count)
        {
            if (buffer is null)
                throw new ArgumentNullException(nameof(buffer));
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (buffer.Length - index < count)
                throw new ArgumentOutOfRangeException("Invalid offset length.");

            try
            {
                var n = Reader.read(buffer, index, count);
                if (n <= 0)
                    return 0;

                return n;
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

#if NET

        /// <inheritdoc />
        public override int Read(Span<char> buffer)
        {
            var l = buffer.Length;
            var b = ArrayPool<char>.Shared.Rent(l);

            try
            {
                var n = Reader.read(b, 0, l);
                if (n <= 0)
                    return 0;

                b.AsSpan().Slice(0, n).CopyTo(buffer);
                return n;
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
            finally
            {
                if (b is not null)
                    ArrayPool<char>.Shared.Return(b);
            }
        }

#endif

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _reader?.close();
            }

            base.Dispose(disposing);
        }

    }

}
