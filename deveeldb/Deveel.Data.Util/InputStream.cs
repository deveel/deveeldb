//  
//  InputStream.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
	/// A utility class that defines the properties of a <see cref="Stream"/>
	/// that should be only read.
	/// </summary>
	public abstract class InputStream : Stream {
		public override bool CanRead {
			get { return true; }
		}

		public override bool CanWrite {
			get { return false; }
		}

		/// <summary>
		/// When overridden, informs if the current stream supports
		/// marking the read position.
		/// </summary>
		public virtual bool MarkSupported {
			get { return false; }
		}

		/// <summary>
		/// Gets the length still available to be read.
		/// </summary>
		public virtual int Available {
			get { return (int) (Length - Position); }
		}

		public override void Write(byte[] buffer, int offset, int count) {
			throw new NotSupportedException();
		}

		public override void WriteByte(byte value) {
			throw new NotSupportedException();
		}

		public override void Flush() {
			throw new NotSupportedException();
		}

		/// <summary>
		/// Marks the current read position of the stream of
		/// the limit of bytes given.
		/// </summary>
		/// <param name="readLimit"></param>
		public virtual void Mark(int readLimit) {
			
		}

		/// <summary>
		/// Resents the current position of the stream to the one
		/// marked by the call to <see cref="Mark"/>.
		/// </summary>
		public virtual void Reset() {
		}

		/// <summary>
		/// Skips the number of bytes given within the stream.
		/// </summary>
		/// <param name="byteCount">The number of bytes to skip from
		/// the read.</param>
		/// <returns>
		/// Returns the actual number of bytes skipped from the stream.
		/// </returns>
		public virtual long Skip(long byteCount) {
			return 0;
		}
	}
}