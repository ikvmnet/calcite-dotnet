using System;
using System.Buffers;
using System.IO;

using java.io;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Implementation of a <see cref="Stream"/> that reads from a Java <see cref="InputStream"/>.
    /// </summary>
    class CalciteInputStream : Stream
    {

        readonly InputStream _input;
        readonly long _length;
        long _position;
        PushbackInputStream? _stream;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="length"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public CalciteInputStream(InputStream input, long length = -1)
        {
            _input = input ?? throw new ArgumentNullException(nameof(input));
            _length = length;
            _position = 0;
        }

        /// <summary>
        /// Initializes and retrieves the <see cref="PushbackReader"/>.
        /// </summary>
        PushbackInputStream Stream => _stream ??= new PushbackInputStream(_input, 1);

        /// <inheritdoc />
        public override bool CanRead => true;

        /// <inheritdoc />
        public override bool CanSeek => _length > -1;

        /// <inheritdoc />
        public override bool CanWrite => false;

        /// <inheritdoc />
        public override long Length => _length > -1 ? _length : throw new NotSupportedException();

        /// <inheritdoc />
        public override long Position
        {
            get => _position;
            set => Seek(value, SeekOrigin.Begin);
        }

        /// <inheritdoc />
        public override int ReadByte()
        {
            try
            {
                var v = Stream.read();
                _position++;
                return v;
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer is null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (buffer.Length - offset < count)
                throw new ArgumentOutOfRangeException("Invalid offset length.");

            try
            {
                var n = Stream.read(buffer, offset, count);
                if (n <= 0)
                    return 0;

                _position += n;
                return n;
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

#if NET

        /// <inheritdoc />
        public override int Read(Span<byte> buffer)
        {
            var l = buffer.Length;
            var b = ArrayPool<byte>.Shared.Rent(l);

            try
            {
                var n = Stream.read(b, 0, l);
                if (n <= 0)
                    return 0;
                    
                _position += n;
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
                    ArrayPool<byte>.Shared.Return(b);
            }
        }

#endif

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin)
        {
            try
            {
                switch (origin)
                {
                    case SeekOrigin.Current:
                        var n1 = Stream.skip(offset);
                        _position += n1;
                        return n1;
                    case SeekOrigin.Begin:
                        Stream.reset();
                        _position = 0;
                        var n2 = Stream.skip(offset);
                        _position += n2;
                        return n2;
                    case SeekOrigin.End:
                        throw new NotSupportedException("Seeking from the end is not supported.");
                    default:
                        throw new InvalidOperationException();
                }
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override void Flush()
        {
            throw new NotSupportedException();
        }

    }

}