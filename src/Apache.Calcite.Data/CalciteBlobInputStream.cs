using System;
using System.IO;

using java.io;
using java.sql;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Implementation of <see cref="Stream"/> that reads data from a <see cref="Blob"/>.
    /// </summary>
    class CalciteBlobInputStream : Stream
    {

        readonly Blob _blob;
        CalciteInputStream? _stream;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="blob"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public CalciteBlobInputStream(Blob blob)
        {
            _blob = blob ?? throw new ArgumentNullException(nameof(blob));
        }

        /// <summary>
        /// Initializes and retrieves the <see cref="PushbackReader"/>.
        /// </summary>
        CalciteInputStream Stream => _stream ??= new CalciteInputStream(_blob.getBinaryStream(), _blob.length());

        /// <inheritdoc />
        public override bool CanRead => Stream.CanRead;

        /// <inheritdoc />
        public override bool CanSeek => Stream.CanSeek;

        /// <inheritdoc />
        public override bool CanWrite => false;

        /// <inheritdoc />
        public override long Length => _blob.length();

        /// <inheritdoc />
        public override long Position
        {
            get => Stream.Position;
            set => Stream.Position = value;
        }

        /// <inheritdoc />
        public override int ReadByte() => Stream.ReadByte();

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count) => Stream.Read(buffer, offset, count);

#if NET

        /// <inheritdoc />
        public override int Read(Span<byte> buffer) => Stream.Read(buffer);

#endif

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin) => Stream.Seek(offset, origin);

        public override void SetLength(long value) => Stream.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => Stream.Write(buffer, offset, count);

        /// <inheritdoc />
        public override void Flush() => Stream.Flush();

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _stream?.Dispose();
                _blob.free();
            }

            base.Dispose(disposing);
        }

    }

}
