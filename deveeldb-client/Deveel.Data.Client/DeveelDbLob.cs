using System;
using System.Data.SqlTypes;
using System.IO;
using System.Text;

namespace Deveel.Data.Client {
	public sealed class DeveelDbLob : Stream, INullable, ISizeable {
		private DeveelDbLob() {
			isNull = true;
		}

		internal DeveelDbLob(Driver driver, int resultId, LargeObjectRef objectRef) {
			this.driver = driver;
			this.resultId = resultId;
			this.objectRef = objectRef;
			size = objectRef.Size;
			access = FileAccess.Read;
			baseStream = new MemoryStream(BufferSize);
			fromServer = true;
		}

		internal DeveelDbLob(Driver driver, Stream baseStream, ReferenceType type, long size, bool establishRef) {
			if (driver == null)
				throw new ArgumentNullException("driver");

			if (establishRef)
				objectRef = driver.CreateLargeObject(this, (int)size, type);

			this.driver = driver;
			this.type = type;
			this.size = size;
			this.baseStream = baseStream;
			access = FileAccess.ReadWrite;
			fromServer = false;
		}

		public DeveelDbLob(DeveelDbConnection connection, Stream baseStream, ReferenceType type, long size)
			: this(connection.Driver, baseStream, type, size, true) {
		}

		public DeveelDbLob(DeveelDbConnection connection, ReferenceType type, long size)
			: this(connection, new MemoryStream(BufferSize), type, size) {
		}

		private readonly Driver driver;

		private readonly bool isNull;
		private readonly LargeObjectRef objectRef;
		private readonly FileAccess access;

		private readonly ReferenceType type;
		private readonly int resultId;

		private long position;
		private readonly Stream baseStream;
		private readonly long size;
		private long buffer_pos = -1;
		private readonly bool fromServer;

		private const int BufferSize = 64 * 1024;

		public static readonly new DeveelDbLob Null = new DeveelDbLob();

		private void FillBuffer(long pos) {
			long readPos = (pos / BufferSize) * BufferSize;
			int toRead = (int)System.Math.Min(BufferSize, (size - readPos));
			if (toRead > 0) {
				ReadPageContent(readPos, toRead);
				buffer_pos = readPos;
			}
		}

		private void ReadPageContent(long pos, int length) {
			try {
				// Request a part of the blob from the server
				byte[] buffer = driver.GetLargeObjectPart(resultId, objectRef.Id, pos, length);
				baseStream.SetLength(0);
				baseStream.Write(buffer, 0, length);
				baseStream.Seek(0, SeekOrigin.Begin);
			} catch (DeveelDbException e) {
				throw new IOException("ADO.NET Error: " + e.Message);
			}
		}

		#region Overrides of Stream

		public override void Flush() {
			if (!fromServer)
				baseStream.Flush();
		}

		public override long Seek(long offset, SeekOrigin origin) {
			if (offset < 0)
				throw new NotSupportedException("Backward seeking not supported.");

			if (origin == SeekOrigin.End)
				throw new NotSupportedException("Seeking from end of the stream is not yet supported.");

			if (!fromServer) {
				position = baseStream.Seek(offset, origin);
				return position;
			}

			if (origin == SeekOrigin.Begin && offset <= position)
				position = offset;
			if (origin == SeekOrigin.Current && offset + position <= size)
				position += offset;

			if (buffer_pos == -1 || (position - buffer_pos) > BufferSize) {
				FillBuffer((position / BufferSize) * BufferSize);
			}

			return position;
		}

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count) {
			if (!CanRead)
				throw new InvalidOperationException("This stream is not readable.");

			if (count <= 0)
				throw new ArgumentException();

			if (!fromServer)
				return baseStream.Read(buffer, offset, count);

			if (buffer_pos == -1)
				FillBuffer(position);

			int p = (int)(position - buffer_pos);
			long buffer_end = System.Math.Min(buffer_pos + BufferSize, size);
			int to_read = (int)System.Math.Min((long)count, buffer_end - position);
			if (to_read <= 0) {
				return 0;
			}
			int has_read = 0;
			while (to_read > 0) {
				baseStream.Seek(p, SeekOrigin.Begin);
				baseStream.Read(buffer, offset, to_read);
				// Array.Copy(buf, p, buffer, offset, to_read);
				has_read += to_read;
				p += to_read;
				offset += to_read;
				count -= to_read;
				position += to_read;
				if (p >= BufferSize) {
					FillBuffer(buffer_pos + BufferSize);
					p -= BufferSize;
				}
				buffer_end = System.Math.Min(buffer_pos + BufferSize, size);
				to_read = (int)System.Math.Min((long)count, buffer_end - position);
			}
			return has_read;
		}

		public override void Write(byte[] buffer, int offset, int count) {
			if (!CanWrite)
				throw new InvalidOperationException("This stream is not writeable.");

			if (count + position > size)
				throw new ArgumentOutOfRangeException("count");

			/*
			byte[] buf = new byte[BufferSize];
			// Fill the buffer
			int blockWrite = (int)System.Math.Min((long)BufferSize, (size - position));
			Array.Copy(buffer, offset, buf, 0, blockWrite);

			driver.PushLargeObjectPart(type, objectId, size, buf, position, blockWrite);
			position += blockWrite;
			*/

			baseStream.Write(buffer, offset, count);
			position += count;

		}

		public override bool CanRead {
			get { return (access & FileAccess.Read) != 0; }
		}

		public override bool CanSeek {
			get { return CanRead; }
		}

		public override bool CanWrite {
			get { return (access & FileAccess.Write) != 0; }
		}

		public override long Length {
			get { return size; }
		}

		public override long Position {
			get { return position; }
			set { throw new NotSupportedException(); }
		}

		public ReferenceType Type {
			get { return type; }
		}

		#endregion

		#region Implementation of INullable

		public bool IsNull {
			get { return isNull; }
		}

		#endregion

		public byte[] GetBytes(long offset, int length) {
			if (offset < 0 || offset + length > Length)
				throw new DeveelDbException("Out of bounds.");

			byte[] buf = new byte[length];

			// The buffer we are reading into
			try {
				Seek(offset, SeekOrigin.Begin);
				for (int i = 0; i < length; ++i) {
					buf[i] = (byte)ReadByte();
				}
			} catch (IOException e) {
				throw new DeveelDbException("IO Error: " + e.Message);
			}

			return buf;
		}

		public string GetString(long offset, int length) {
			Seek(offset, SeekOrigin.Begin);
			StreamReader reader = new StreamReader(this, type == ReferenceType.Ascii ? Encoding.ASCII : Encoding.Unicode);
			StringBuilder sb = new StringBuilder(length);
			for (int i = 0; i < length; i++) {
				int ch = reader.Read();
				if (ch == -1)
					break;

				sb.Append((char)ch);
			}

			return sb.ToString();
		}

		#region Implementation of ISizeable

		int ISizeable.Size {
			get { return (int) Length; }
		}

		#endregion
	}
}