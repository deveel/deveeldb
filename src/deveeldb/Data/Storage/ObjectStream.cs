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

namespace Deveel.Data.Storage {
	public sealed class ObjectStream : Stream {
		private readonly ILargeObject largeObject;
		private long position;
		private Stream outTempStream;

		private long readBufferPos;
		private readonly byte[] readBuf;

		private const int BufferSize = 64*1024;

		public ObjectStream(ILargeObject largeObject) {
			this.largeObject = largeObject ?? throw new ArgumentNullException(nameof(largeObject));
			outTempStream = new MemoryStream(64*1024);

			readBuf = new byte[BufferSize];
			readBufferPos = -1;
		}

		private void ReadPageContent(byte[] buffer, long pos, int length) {
			largeObject.Read(pos, buffer, length);
		}

		private void FillBuffer(long pos) {
			long readPos = (pos/BufferSize)*BufferSize;
			int toRead = (int) System.Math.Min(BufferSize, (largeObject.RawSize - readPos));
			if (toRead > 0) {
				ReadPageContent(readBuf, readPos, toRead);
				readBufferPos = readPos;
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (outTempStream != null)
					outTempStream.Dispose();
			}

			outTempStream = null;
			base.Dispose(disposing);
		}

		public override void Flush() {
			if (outTempStream == null ||
			    outTempStream.Length == 0)
				return;

			try {
				long offset = 0;
				var buffer = new byte[BufferSize];
				var totalLength = outTempStream.Length;

				outTempStream.Seek(0, SeekOrigin.Begin);

				while (offset < totalLength) {
					// Fill the buffer
					int index = 0;
					int blockRead = (int) System.Math.Min((long) BufferSize, (totalLength - offset));
					int toRead = blockRead;
					while (toRead > 0) {
						int count = outTempStream.Read(buffer, index, toRead);
						if (count == 0)
							break;

						index += count;
						toRead -= count;
					}

					// Send the part of the streamable object to the database.
					largeObject.Write(offset, buffer, blockRead);
					// Increment the offset and upload the next part of the object.
					offset += blockRead;
				}
			} finally {
				outTempStream.SetLength(0);
			}
		}

		public override long Seek(long offset, SeekOrigin origin) {
			throw new NotImplementedException();
		}

		public override void SetLength(long value) {
			throw new NotSupportedException("The length of the underlying object cannot be changed.");
		}

		public override int Read(byte[] buffer, int offset, int count) {
			if (!largeObject.IsComplete)
				throw new IOException("The underlying object is not complete.");

			if (count <= 0) {
				return 0;
			}

			if (readBufferPos == -1) {
				FillBuffer(position);
			}

			int p = (int) (position - readBufferPos);
			long bufferEnd = System.Math.Min(readBufferPos + BufferSize, largeObject.RawSize);
			int toRead = (int) System.Math.Min((long) count, bufferEnd - position);
			if (toRead <= 0) {
				return 0;
			}
			int hasRead = 0;
			while (toRead > 0) {
				Array.Copy(readBuf, p, buffer, offset, toRead);
				hasRead += toRead;
				p += toRead;
				offset += toRead;
				count -= toRead;
				position += toRead;
				if (p >= BufferSize) {
					FillBuffer(readBufferPos + BufferSize);
					p -= BufferSize;
				}
				bufferEnd = System.Math.Min(readBufferPos + BufferSize, largeObject.RawSize);
				toRead = (int) System.Math.Min((long)count, bufferEnd - position);
			}

			return hasRead;
		}

		public override void Write(byte[] buffer, int offset, int count) {
			if (largeObject.IsComplete)
				throw new IOException("The underlying object is complete.");

			outTempStream.Write(buffer, offset, count);
			position += count;
		}

		public override bool CanRead => largeObject.IsComplete;

		public override bool CanSeek => true;

		public override bool CanWrite => !largeObject.IsComplete;

		public override long Length => largeObject.RawSize;

		public override long Position {
			get { return position; }
			set { position = Seek(value, SeekOrigin.Begin); }
		}
	}
}