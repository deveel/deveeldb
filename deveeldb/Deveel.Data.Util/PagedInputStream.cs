//  
//  PagedInputStream.cs
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

namespace Deveel.Data.Util {
	/// <summary>
	/// A <see cref="Stream"/> that reads data from an underlying
	/// representation in fixed sized pages.
	/// </summary>
	/// <remarks>
	/// This object maintains a single buffer that is the size of a page.
	/// <para>
	/// This implementation supports <see cref="Mark"/> and buffered access 
	/// to the data.
	/// </para>
	/// <para>
	/// The only method that needs to be implemented is the <see cref="ReadPageContent"/>
	/// method.
	/// </para>
	/// </remarks>
	public abstract class PagedInputStream : InputStream {
		/// <summary>
		/// The size of the buffer page.
		/// </summary>
		private readonly int BUFFER_SIZE;

		/// <summary>
		/// The current position in the stream.
		/// </summary>
		private long position;

		/// <summary>
		/// The total size of the underlying dataset.
		/// </summary>
		private readonly long size;

		/// <summary>
		/// The start buffer position.
		/// </summary>
		private long buffer_pos;

		/// <summary>
		/// The buffer.
		/// </summary>
		private readonly byte[] buf;

		/// <summary>
		/// Last marked position.
		/// </summary>
		private long mark_position;

		/// <summary>
		/// Constructs the input stream.
		/// </summary>
		/// <param name="page_size">The size of the pages when accessing 
		/// the underlying stream.</param>
		/// <param name="total_size">The total size of the underlying 
		/// data set.</param>
		protected PagedInputStream(int page_size, long total_size) {
			BUFFER_SIZE = page_size;
			position = 0;
			size = total_size;
			mark_position = 0;
			buf = new byte[BUFFER_SIZE];
			buffer_pos = -1;
		}

		/// <summary>
		/// Reads the page at the given offset in the underlying data into the 
		/// given byte[] array.
		/// </summary>
		/// <param name="buf"></param>
		/// <param name="pos">The starting position within the stream where to 
		/// start to read. is guarenteed to be a multiple of 
		/// <see cref="BUFFER_SIZE">buffer size</see></param>
		/// <param name="length">The number of bytes to read from the page. This
		/// must be either equal or smaller than <see cref="BUFFER_SIZE">buffer size</see>,
		/// if the page to read contains the end of the stream.</param>
		/// <remarks>
		/// </remarks>
		protected abstract void ReadPageContent(byte[] buf, long pos, int length);

		/// <summary>
		/// Fills the buffer with data from the blob at the given position.
		/// </summary>
		/// <param name="pos"></param>
		/// <remarks>
		/// A buffer may be partially filled if the end is reached.
		/// </remarks>
		private void FillBuffer(long pos) {
			long read_pos = (pos / BUFFER_SIZE) * BUFFER_SIZE;
			int to_read = (int)System.Math.Min((long)BUFFER_SIZE, (size - read_pos));
			if (to_read > 0) {
				ReadPageContent(buf, read_pos, to_read);
				buffer_pos = read_pos;
			}
		}

		// ---------- Implemented from InputStream ----------

		/// <inheritdoc/>
		public override bool CanSeek {
			get { return true; }
		}

		/// <inheritdoc/>
		public override long Position {
			get { return position; }
			set { throw new NotSupportedException(); }
		}

		/// <inheritdoc/>
		public override long Length {
			get { return size; }
		}

		/// <inheritdoc/>
		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		/// <inheritdoc/>
		public override long Seek(long offset, SeekOrigin origin) {
			if (offset < 0)
				throw new NotSupportedException("Backward seeking not supported.");

			if (origin == SeekOrigin.End)
				throw new NotSupportedException("Seeking from end of the stream is not yet supported.");

			long toSkip;
			if (origin == SeekOrigin.Current) {
				if (offset == 0)
					return position;

				if (offset < position)
					throw new ArgumentException("The offset cannot be smaller than the current position " +
					                            "when seeking from the current position of the stream.");

				toSkip = offset - position;

				if (position + toSkip >= size)
					throw new ArgumentException("The offset ");
			} else {
				if (offset > size)
					throw new ArgumentException();

				toSkip = offset;
			}

			Skip(toSkip);

			return position;
		}

		/// <inheritdoc/>
		public override int ReadByte() {
			if (position >= size) {
				return -1;
			}

			if (buffer_pos == -1) {
				FillBuffer(position);
			}

			int p = (int)(position - buffer_pos);
			int v = ((int)buf[p]) & 0x0FF;

			++position;
			// Fill the next part of the buffer?
			if (p + 1 >= BUFFER_SIZE) {
				FillBuffer(buffer_pos + BUFFER_SIZE);
			}

			return v;
		}

		/// <inheritdoc/>
		public override int Read(byte[] read_buf, int off, int len) {
			if (len <= 0) {
				return 0;
			}

			if (buffer_pos == -1) {
				FillBuffer(position);
			}

			int p = (int)(position - buffer_pos);
			long buffer_end = System.Math.Min(buffer_pos + BUFFER_SIZE, size);
			int to_read = (int)System.Math.Min((long)len, buffer_end - position);
			if (to_read <= 0) {
				return -1;
			}
			int has_read = 0;
			while (to_read > 0) {
				Array.Copy(buf, p, read_buf, off, to_read);
				has_read += to_read;
				p += to_read;
				off += to_read;
				len -= to_read;
				position += to_read;
				if (p >= BUFFER_SIZE) {
					FillBuffer(buffer_pos + BUFFER_SIZE);
					p -= BUFFER_SIZE;
				}
				buffer_end = System.Math.Min(buffer_pos + BUFFER_SIZE, size);
				to_read = (int)System.Math.Min((long)len, buffer_end - position);
			}
			return has_read;
		}

		/// <inheritdoc/>
		public override long Skip(long n) {
			long act_skip = System.Math.Min(n, size - position);

			if (n < 0) {
				throw new IOException("Negative skip");
			}
			position += act_skip;
			if (buffer_pos == -1 || (position - buffer_pos) > BUFFER_SIZE) {
				FillBuffer((position / BUFFER_SIZE) * BUFFER_SIZE);
			}

			return act_skip;
		}

		/// <inheritdoc/>
		public override int Available {
			get { return (int)System.Math.Min((long)Int32.MaxValue, (size - position)); }
		}

		/// <inheritdoc/>
		public override void Close() {
		}

		/// <inheritdoc/>
		public override void Mark(int limit) {
			mark_position = position;
		}

		/// <inheritdoc/>
		public override void Reset() {
			position = mark_position;
			long fill_pos = (position / BUFFER_SIZE) * BUFFER_SIZE;
			if (fill_pos != buffer_pos) {
				try {
					FillBuffer(fill_pos);
				} catch (IOException e) {
					throw new ApplicationException(e.Message);
				}
			}
		}

		/// <inheritdoc/>
		public override bool MarkSupported {
			get { return true; }
		}

	}
}