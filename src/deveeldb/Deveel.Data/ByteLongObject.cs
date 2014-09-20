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
using System.IO;
using System.Text;

namespace Deveel.Data {
	/// <summary>
	/// A byte array that can be transferred between the client and server.
	/// </summary>
	/// <remarks>
	/// This is used for transferring BLOB data to/from the database engine.
	/// </remarks>
	[Serializable]
	public class ByteLongObject : IBlobAccessor {
		/// <summary>
		/// The binary data.
		/// </summary>
		private readonly byte[] data;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="from"></param>
		/// <param name="offset"></param>
		/// <param name="length"></param>
		public ByteLongObject(byte[] from, int offset, int length) {
			data = new byte[length];
			Array.Copy(from, offset, data, 0, length);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="from"></param>
		public ByteLongObject(byte[] from)
			: this(from, 0, from.Length) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="input"></param>
		/// <param name="length"></param>
		public ByteLongObject(Stream input, int length) {
			data = new byte[length];

			int i = 0;
			while (i < length) {
				int read = input.Read(data, i, length - i);
				if (read == 0)
					throw new EndOfStreamException();

				i += read;
			}
		}

		/// <summary>
		/// Returns the size of the data in this object.
		/// </summary>
		public int Length {
			get { return data.Length; }
		}

		/// <summary>
		/// Returns the byte at offset <paramref name="n"/> into the binary object.
		/// </summary>
		/// <param name="n"></param>
		/// <returns></returns>
		public byte this[int n] {
			get { return data[n]; }
		}


		/// <summary>
		/// Returns the internal byte of this binary object.
		/// </summary>
		/// <remarks>
		/// Care needs to be taken when handling this object because 
		/// altering the contents will change this object.
		/// </remarks>
		/// <returns>
		/// </returns>
		public byte[] ToArray() {
			return (byte[]) data.Clone();
		}

		/// <summary>
		/// Returns a stream that allows us to read the entire byte long object.
		/// </summary>
		/// <returns></returns>
		public Stream GetInputStream() {
			return new BlobInputStream(this);
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			if (data == null) {
				buf.Append("[ BLOB (NULL) ]");
			} else {
				buf.Append("[ BLOB size=");
				buf.Append(data.Length);
				buf.Append(" ]");
			}
			return buf.ToString();
		}

		/// <summary>
		/// Inner class that encapsulates the byte long object in an input stream.
		/// </summary>
		private class BlobInputStream : Stream {
			private readonly ByteLongObject blob;
			private int index;

			public BlobInputStream(ByteLongObject blob) {
				index = 0;
				this.blob = blob;
			}

			public override int ReadByte() {
				if (index >= blob.Length) {
					return -1;
				}
				int b = ((int)blob[index]) & 0x0FF;
				++index;
				return b;
			}

			public override bool CanSeek {
				get { return false; }
			}

			public override bool CanRead {
				get { return true; }
			}

			public override bool CanWrite {
				get { return false; }
			}

			public override long Length {
				get { return blob.Length; }
			}

			public override long Position {
				get { return index; }
				set {
					if (value >= blob.Length)
						throw new ArgumentOutOfRangeException();
					if (value > Int32.MaxValue)
						throw new ArgumentOutOfRangeException();
					index = (int)value;
				}
			}

			public override long Seek(long offset, SeekOrigin origin) {
				throw new NotSupportedException();
			}

			public override void SetLength(long value) {
				throw new NotSupportedException();
			}

			public override void Write(byte[] buffer, int offset, int count) {
				throw new NotSupportedException();
			}

			public override void Flush() {
			}

			public override int Read(byte[] buf, int off, int len) {
				// As per the InputStream specification.
				if (len <= 0)
					throw new ArgumentException();

				int size = blob.Length;
				int toRead = System.Math.Min(len, size - index);

				if (toRead <= 0)
					// Nothing can be Read
					return 0;

				Array.Copy(blob.data, index, buf, off, toRead);
				index += toRead;

				return toRead;
			}
		}
	}
}