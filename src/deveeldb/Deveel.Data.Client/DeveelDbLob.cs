// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.IO;

using Deveel.Data.Protocol;
using Deveel.Data.Store;

namespace Deveel.Data.Client {
	public sealed class DeveelDbLob : Stream {
		private FileAccess access;
		private ObjectId objId;
		private DeveelDbConnection connection;

		private Stream writeStream;
		private ILargeObjectChannel lobChannel;
		private Stream readStream;
		private long pos;
		private long size;


		private DeveelDbLob(ObjectId objId, long size, FileAccess access) {
			this.objId = objId;
			this.access = access;
			this.size = size;

			if (access == FileAccess.Write)
				writeStream = new MemoryStream();
		}

		public DeveelDbLob(long size)
			: this(new ObjectId(), size, FileAccess.Write) {
		}

		internal DeveelDbLob(DeveelDbConnection connection, ObjectId objId, long size)
			: this(objId, size, FileAccess.Read) {
			this.connection = connection;
			lobChannel = connection.OpenLargeObjectChannel(objId);
			readStream = new BufferedStream(new LobInputStream(this), (int)(size/64));
		}

		public override void Flush() {
			if (writeStream != null)
				writeStream.Flush();
		}

		public override long Seek(long offset, SeekOrigin origin) {
			if (origin == SeekOrigin.Begin) {
				if (offset < 0 || offset >= size)
					throw new ArgumentOutOfRangeException("offset");

				pos = offset;
			} else if (origin == SeekOrigin.End) {
				if (offset < 0 || pos - offset < 0)
					throw new ArgumentOutOfRangeException("offset");

				pos -= offset;
			} else {
				if (pos + offset >= size)
					throw new ArgumentOutOfRangeException("offset");

				pos += offset;
			}

			return pos;
		}

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count) {
			if (!CanRead)
				throw new NotSupportedException("The stream is not readable");

			return readStream.Read(buffer, offset, count);
		}

		public override void Write(byte[] buffer, int offset, int count) {
			if (!CanWrite)
				throw new NotSupportedException("The stream is not writable");

			if (pos + count > size)
				throw new IOException("Writing over the maximum allowed size of the LOB");

			writeStream.Write(buffer, offset, count);
			pos += count;
		}

		public override bool CanRead {
			get { return access == FileAccess.Read; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		public override bool CanWrite {
			get { return access == FileAccess.Write; }
		}

		public override long Length {
			get { return size; }
		}

		public override long Position {
			get { return pos; }
			set { throw new NotSupportedException(); }
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (writeStream != null)
					writeStream.Dispose();

				if (readStream != null)
					readStream.Dispose();

				if (lobChannel != null)
					lobChannel.Dispose();

				if (connection != null)
					connection.DisposeObject(objId);
			}

			lobChannel = null;
			writeStream = null;
			readStream = null;
			base.Dispose(disposing);
		}

		internal static ObjectId Upload(DeveelDbConnection connection, DeveelDbLob lob) {
			if (connection == null)
				throw new InvalidOperationException("An open connection is required to upload the LOB");

			var objId = connection.CreateLargeObject(lob.Length);
			using (var channel = connection.OpenLargeObjectChannel(objId)) {
				const int bufferSize = 2048;
				var copyBuffer = new byte[bufferSize];
				int readCount;
				long copyOffset = 0;
				lob.writeStream.Seek(0, SeekOrigin.Begin);

				while ((readCount = lob.writeStream.Read(copyBuffer, 0, bufferSize)) > 0) {
					channel.PushData(copyOffset, copyBuffer, readCount);
					copyOffset += readCount;
				}
			}

			return objId;
		}

		#region LobInputStream

		class LobInputStream : Stream {
			private DeveelDbLob lob;

			public LobInputStream(DeveelDbLob lob) {
				this.lob = lob;
			}

			public override void Flush() {
			}

			public override long Seek(long offset, SeekOrigin origin) {
				throw new NotSupportedException();
			}

			public override void SetLength(long value) {
				throw new NotSupportedException();
			}

			public override int Read(byte[] buffer, int offset, int count) {
				var readCount = (int) System.Math.Min(Length, count);

				var data = lob.lobChannel.ReadData(lob.pos, readCount);

				Array.Copy(data, 0, buffer, offset, data.Length);

				lob.pos += data.Length;
				return data.Length;
			}

			public override void Write(byte[] buffer, int offset, int count) {
				throw new NotSupportedException();
			}

			public override bool CanRead {
				get { return true; }
			}

			public override bool CanSeek {
				get { return false; }
			}

			public override bool CanWrite {
				get { return false; }
			}

			public override long Length {
				get { return lob.size; }
			}

			public override long Position {
				get { return lob.pos; }
				set { lob.Seek(value, SeekOrigin.Begin); }
			}

			protected override void Dispose(bool disposing) {
				lob = null;
				base.Dispose(disposing);
			}
		}

		#endregion
	}
}
