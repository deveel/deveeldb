// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Data;
using System.IO;
using System.Text;

using Deveel.Data.Protocol;

namespace Deveel.Data.Client {
	/// <summary>
	/// Represents a *LOB (Large OBject) that can be streamed
	/// in and out from/to the database.
	/// </summary>
	public class DeveelDbLob : Stream {
		/// <summary>
		/// Constructs a <see cref="DeveelDbLob"/> when retrieved from the database.
		/// </summary>
		/// <param name="connection">The connection used to retrieve the LOB.</param>
		/// <param name="resultId">The identification number of the result-set
		/// on the server to which this LOB belongs to.</param>
		/// <param name="objectRef"></param>
		internal DeveelDbLob(DeveelDbConnection connection, int resultId, StreamableObject objectRef) {
			this.connection = connection;
			this.resultId = resultId;
			this.objectRef = objectRef;
			size = objectRef.Size;
			access = FileAccess.Read;
			baseStream = new MemoryStream(BufferSize);
			fromServer = true;
		}

		internal DeveelDbLob(DeveelDbCommand command, Stream baseStream, ReferenceType type, long size, bool establishRef) {
			if (command == null)
				throw new ArgumentNullException("command");
			if (command.Connection == null)
				throw new ArgumentException("The command must already be contained in a Connection at this point.");

			if (establishRef)
				objectRef = command.CreateStreamableObject(this, (int)size, type);

			connection = command.Connection;
			this.type = type;
			this.size = size;
			this.baseStream = baseStream;
			access = FileAccess.ReadWrite;
			fromServer = false;
		}

		public DeveelDbLob(DeveelDbCommand command, Stream baseStream, ReferenceType type, long size)
			: this(command, baseStream, type, size, true) {
		}

		public DeveelDbLob(DeveelDbCommand command, ReferenceType type, long size)
			: this(command, new MemoryStream(BufferSize), type, size) {
		}

		/// <summary>
		/// The <see cref="DeveelDbCommand"/> object that this object was 
		/// returned as part of the result of.
		/// </summary>
		private readonly DeveelDbConnection connection;

		private readonly StreamableObject objectRef;
		private readonly FileAccess access;

		private readonly ReferenceType type;

		/// <summary>
		/// The result_id of the ResultSet this clob is from.
		/// </summary>
		private readonly int resultId;

		/// <summary>
		/// The current position in the stream.
		/// </summary>
		private long position;

		private readonly Stream baseStream;

		/// <summary>
		/// The start buffer position.
		/// </summary>
		private long buffer_pos = -1;

		private readonly bool fromServer;

		private const int BufferSize = 64 * 1024;


		/// <summary>
		/// The size of the streamable object.
		/// </summary>
		private readonly long size;

		public override bool CanRead {
			get { return (access & FileAccess.Read) != 0; }
		}

		public override bool CanWrite {
			get { return (access & FileAccess.Write) != 0; }
		}

		public override bool CanSeek {
			get { return CanRead; }
		}

		/// <summary>
		/// Gets the type of the object backed by the stream.
		/// </summary>
		public ReferenceType Type {
			get { return type; }
		}

		internal StreamableObject ObjectRef {
			get { return objectRef; }
		}


		/// <summary>
		/// Gets the number of bytes in this streamable object.
		/// </summary>
		/// <remarks>
		/// This may not represent the actual size of the object when it is 
		/// decoded.  For example, a IClob may be encoded as 2-byte per character 
		/// (unicode) so the actual length of the clob with be size / 2.
		/// </remarks>
		public override long Length {
			get { return size; }
		}

		/// <summary>
		/// Fills the buffer with data from the blob at the given position.
		/// </summary>
		/// <param name="pos"></param>
		/// <remarks>
		/// A buffer may be partially filled if the end is reached.
		/// </remarks>
		private void FillBuffer(long pos) {
			long read_pos = (pos / BufferSize) * BufferSize;
			int to_read = (int)System.Math.Min((long)BufferSize, (size - read_pos));
			if (to_read > 0) {
				ReadPageContent(read_pos, to_read);
				buffer_pos = read_pos;
			}
		}

		private void ReadPageContent(long pos, int length) {
			try {
				// Request a part of the blob from the server
				byte[] buffer = connection.RequestStreamableObjectPart(resultId, objectRef.Identifier, pos, length);
				baseStream.SetLength(0);
				baseStream.Write(buffer, 0, length);
				baseStream.Seek(0, SeekOrigin.Begin);
			} catch (DataException e) {
				throw new IOException("SQL Error: " + e.Message);
			}
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

		public override void Flush() {
			// when writing into this stream the data are automaticaly flushed
			// to the database...
			if (!fromServer)
				baseStream.Flush();
		}

		public override void Close() {
			//TODO:
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				//TODO: dispose the object on the server...
			}

			base.Dispose(disposing);
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

			connection.PushStreamableObjectPart(type, objectId, size, buf, position, blockWrite);
			position += blockWrite;
			*/

			baseStream.Write(buffer, offset, count);
			position += count;
		}

		public override int Read(byte[] buffer, int offset, int count) {
			if (!CanRead)
				throw new InvalidOperationException("This stream is not readable.");

			if (count <= 0)
				throw new ArgumentException();

			if (!fromServer)
				return baseStream.Read(buffer, offset, count);

			if (buffer_pos == -1) {
				FillBuffer(position);
			}

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

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		public override long Position {
			get { return position; }
			set { throw new NotSupportedException(); }
		}

		public byte[] GetBytes(long offset, int length) {
			if (offset < 0 || offset + length > Length)
				throw new DataException("Out of bounds.");

			byte[] buf = new byte[length];

			// The buffer we are reading into
			try {
				Seek(offset, SeekOrigin.Begin);
				for (int i = 0; i < length; ++i) {
					buf[i] = (byte) ReadByte();
				}
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				throw new DataException("IO Error: " + e.Message);
			}

			return buf;
		}

		public string GetString(long offset, int length) {
			Seek(offset, SeekOrigin.Begin);
			StreamReader reader = new StreamReader(this, type == ReferenceType.AsciiText ? Encoding.ASCII : Encoding.Unicode);
			StringBuilder sb = new StringBuilder(length);
			for (int i = 0; i < length; i++) {
				int ch = reader.Read();
				if (ch == -1)
					break;

				sb.Append((char)ch);
			}

			return sb.ToString();
		}
	}
}