// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Protocol;

namespace Deveel.Data.Client {
	public class DeveelDbLargeObject : Stream {
		private DeveelDbConnection connection;
		private IStreamableObjectChannel channel;
		private readonly FileAccess access;

		private long position;
		private long length;
		private ReferenceType referenceType;

		private byte[] tempBuffer;
		private long bufferOffset;

		private const int BufferSize = 64 * 1024;

		public DeveelDbLargeObject(ReferenceType referenceType, long length) 
			: this(referenceType, length, FileAccess.ReadWrite) {
		}

		public DeveelDbLargeObject(ReferenceType referenceType, long length, FileAccess access) {
			this.access = access;
			this.length = length;
			this.referenceType = referenceType;
		}


		internal StreamableObject ObjectRef { get; set; }

		public override long Length {
			get { return length; }
		}

		public override long Position {
			get { return position; }
			set {
				position = value;
				Seek(value, SeekOrigin.Begin);
			}
		}

		public DeveelDbConnection Connection {
			get { return connection; }
			set {
				connection = value;
				if (channel == null && connection != null) {
					CreateChannel();
				}
			}
		}

		private IStreamableObjectChannel CreateChannel() {
			if (ObjectRef != null) {
				channel = connection.OpenObjectChannel(ObjectRef.Identifier, ObjectPersistenceType.Volatile);
			} else {
				ObjectRef = connection.CreateStreamableObject(referenceType, length, ObjectPersistenceType.Volatile);
				channel = connection.OpenObjectChannel(ObjectRef.Identifier, ObjectPersistenceType.Volatile);
			}

			return channel;
		}

		public override bool CanRead {
			get { return (access & FileAccess.Read) != 0; }
		}

		public override bool CanWrite {
			get { return (access & FileAccess.Write) != 0; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		private void FillBuffer(long pos) {
			long readPos = (pos / BufferSize) * BufferSize;
			var toRead = (int)System.Math.Min(BufferSize, (length - readPos));
			if (toRead > 0) {
				ReadPageContent(readPos, toRead);
				bufferOffset = readPos;
			}
		}

		private void ReadPageContent(long pos, int readLength) {
			try {
				if (channel == null)
					throw new InvalidOperationException();

				tempBuffer = new byte[readLength];

				// Request a part of the blob from the server
				byte[] buffer = channel.ReadData(pos, readLength);
				Array.Copy(buffer, 0, tempBuffer, 0, buffer.Length);
			} catch (DataException e) {
				throw new IOException("SQL Error: " + e.Message);
			}
		}

		public override long Seek(long offset, SeekOrigin origin) {
			if (offset < 0)
				throw new NotSupportedException("Backward seeking not supported.");

			if (origin == SeekOrigin.End)
				throw new NotSupportedException("Seeking from end of the stream is not yet supported.");

			if (origin == SeekOrigin.Begin && offset <= position)
				position = offset;
			if (origin == SeekOrigin.Current && offset + position <= length)
				position += offset;

			if (bufferOffset == -1 || (position - bufferOffset) > BufferSize) {
				FillBuffer((position/BufferSize)*BufferSize);
			}

			return position;
		}

		public override void Write(byte[] buffer, int offset, int count) {
			if (!CanWrite)
				throw new InvalidOperationException("This stream is not writeable.");

			if (count + position > length)
				throw new ArgumentOutOfRangeException("count");

			if (channel == null)
				channel = CreateChannel();

			var buf = new byte[BufferSize];
			// Fill the buffer
			var blockWrite = (int) System.Math.Min(BufferSize, (length - position));
			Array.Copy(buffer, offset, buf, 0, blockWrite);

			channel.PushData(position, buffer, count);
			position += blockWrite;
		}

		public override int Read(byte[] buffer, int offset, int count) {
			if (!CanRead)
				throw new InvalidOperationException("This stream is not readable.");

			if (count <= 0)
				throw new ArgumentException();

			if (bufferOffset == -1) {
				FillBuffer(position);
			}

			var p = (int)(position - bufferOffset);
			long bufferEnd = System.Math.Min(bufferOffset + BufferSize, length);
			var toRead = (int)System.Math.Min(count, bufferEnd - position);
			if (toRead <= 0)
				return 0;

			int hasRead = 0;
			while (toRead > 0) {
				Array.Copy(tempBuffer, p, buffer, offset, toRead);
				hasRead += toRead;
				p += toRead;
				offset += toRead;
				count -= toRead;
				position += toRead;
				if (p >= BufferSize) {
					FillBuffer(bufferOffset + BufferSize);
					p -= BufferSize;
				}
				bufferEnd = System.Math.Min(bufferOffset + BufferSize, length);
				toRead = (int)System.Math.Min(count, bufferEnd - position);
			}

			return hasRead;
		}

		public override void Flush() {
			if (channel != null)
				channel.Flush();
		}

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (connection != null &&
					ObjectRef != null)
					connection.DisposeObject(ObjectRef.Identifier);

				if (channel != null)
					channel.Dispose();

			}

			base.Dispose(disposing);
		}
	}
}