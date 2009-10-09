//  
//  DbBlob.cs
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
using System.Data;
using System.IO;

namespace Deveel.Data.Client {
	/// <summary>
	/// An implementation of an <see cref="IBlob"/> object.
	/// </summary>
	/// <remarks>
	/// This implementation keeps the entire <see cref="IBlob"/> in memory.
	/// </remarks>
	class DbBlob : IBlob {
		/// <summary>
		/// The <see cref="ByteLongObject"/> that is a container for the data in this blob.
		/// </summary>
		private readonly ByteLongObject blob;

		internal DbBlob(ByteLongObject blob) {
			this.blob = blob;
		}


		public long Length {
			get { return blob.Length; }
		}

		public byte[] GetBytes(long pos, int length) {
			// First byte is at position 1 according to JDBC Spec.
			--pos;
			if (pos < 0 || pos + length > this.Length) {
				throw new DataException("Out of bounds.");
			}

			byte[] buf = new byte[length];
			Array.Copy(blob.ToArray(), (int)pos, buf, 0, length);
			return buf;
		}

		public Stream GetStream() {
			return new MemoryStream(blob.ToArray(), 0, (int) Length);
		}

		public long GetPosition(byte[] pattern, long start) {
			byte[] buf = blob.ToArray();
			int len = (int)Length;
			int max = ((int)Length) - pattern.Length;

			int i = (int)(start - 1);
			while (true) {
				// Look for first byte...
				while (i <= max && buf[i] != pattern[0]) {
					++i;
				}
				// Reached end so exit..
				if (i > max) {
					return -1;
				}

				// Found first character, so look for the rest...
				int search_from = i;
				int found_index = 1;
				while (found_index < pattern.Length &&
				       buf[search_from] == pattern[found_index]) {
					++search_from;
					++found_index;
				}

				++i;
				if (found_index >= pattern.Length) {
					return (long) i;
				}
			}
		}

		public long GetPosition(IBlob pattern, long start) {
			byte[] buf;
			// Optimize if DbBlob,
			buf = ((DbBlob)pattern).blob.ToArray();
			return GetPosition(buf, start);
		}
	}
}