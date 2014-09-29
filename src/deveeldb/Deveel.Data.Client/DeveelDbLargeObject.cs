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
using Deveel.Data.Util;

namespace Deveel.Data.Client {
	public class DeveelDbLargeObject : Stream {
		private DeveelDbConnection connection;
		private IStreamableObjectChannel channel;
		private readonly FileAccess access;

		private long position;
		private long writePos;

		private readonly BlobInputStream inputStream;

		private readonly long length;
		private readonly ReferenceType referenceType;

		private readonly Stream tempStream;

		private const int BufferSize = 64 * 1024;

		public DeveelDbLargeObject(ReferenceType referenceType, long length) 
			: this(referenceType, length, FileAccess.ReadWrite) {
		}

		protected DeveelDbLargeObject(ReferenceType referenceType, long length, FileAccess access) {
			this.access = access;
			this.length = length;
			this.referenceType = referenceType;

			tempStream = new MemoryStream(BufferSize);
		}

		internal DeveelDbLargeObject(StreamableObject obj, DeveelDbConnection connection)
		: this(obj.Type, obj.Size) {
			ObjectRef = obj;
			this.connection = connection;

			channel = connection.OpenObjectChannel(obj.Identifier);
			inputStream = new BlobInputStream(channel, 1024, obj.Size);
		}

		internal StreamableObject ObjectRef { get; private set; }

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
					channel = CreateChannel();
				}
			}
		}

		private IStreamableObjectChannel CreateChannel() {
			if (connection == null)
				throw new InvalidOperationException("The object is outside the context.");

			if (ObjectRef == null)
				ObjectRef = connection.CreateStreamableObject(referenceType, length);

			return connection.OpenObjectChannel(ObjectRef.Identifier);
		}

		public override bool CanRead {
			get { return (access & FileAccess.Read) != 0; }
		}

		public override bool CanWrite {
			get { return (access & FileAccess.Write) != 0; }
		}

		public override bool CanSeek {
			get { return false; }
		}

		public override long Seek(long offset, SeekOrigin origin) {
			throw new NotSupportedException();
		}

		public override void Write(byte[] buffer, int offset, int count) {
			if (!CanWrite)
				throw new InvalidOperationException("This stream is not writeable.");

			lock (tempStream) {
				if (count + position > length)
					throw new ArgumentOutOfRangeException("count");

				tempStream.Write(buffer, offset, count);
				position += count;
			}
		}

		public override int Read(byte[] buffer, int offset, int count) {
			if (!CanRead)
				throw new InvalidOperationException("This stream is not readable.");

			return inputStream.Read(buffer, offset, count);
		}

		#region BlobInputStream

		class BlobInputStream : PagedInputStream {
			private readonly IStreamableObjectChannel channel;

			public BlobInputStream(IStreamableObjectChannel channel, int pageSize, long totalSize) 
				: base(pageSize, totalSize) {
				this.channel = channel;
			}

			protected override void ReadPageContent(byte[] buf, long pos, int length) {
				var data = channel.ReadData(pos, length);
				Array.Copy(data, 0, buf, 0, data.Length);
			}
		}

		#endregion

		public override void Flush() {
			lock (tempStream) {
				if (channel == null)
					channel = CreateChannel();

				var buf = new byte[BufferSize];
				var totalLen = ObjectRef.Size;

				tempStream.Seek(0, SeekOrigin.Begin);

				// Fill the buffer
				int offset = 0;
				int blockRead = (int) System.Math.Min(BufferSize, totalLen);
				int toRead = blockRead;
				while (toRead > 0) {
					int count = tempStream.Read(buf, offset, toRead);
					if (count == 0)
						throw new IOException("Premature end of stream.");

					offset += count;
					toRead -= count;
				}

				tempStream.SetLength(0);

				// Send the part of the streamable object to the database.
				channel.PushData(writePos, buf, blockRead);

				writePos += blockRead;
			}
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