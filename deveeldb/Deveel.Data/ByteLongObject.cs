//  
//  ByteLongObject.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Text;

using Deveel.Data.Util;

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
				if (read == -1) {
					throw new IOException("Premature end of stream.");
				}
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
			return new BLOBInputStream(this);
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
		private class BLOBInputStream : Stream {
			private readonly ByteLongObject blob;
			private int index;

			public BLOBInputStream(ByteLongObject blob) {
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
				int to_read = System.Math.Min(len, size - index);

				if (to_read <= 0) {
					// Nothing can be Read
					return 0;
				}

				Array.Copy(blob.data, index, buf, off, to_read);
				index += to_read;

				return to_read;
			}
		}
	}
}