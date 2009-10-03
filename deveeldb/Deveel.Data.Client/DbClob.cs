// 
//  DbClob.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Client {
	/// <summary>
	/// An implementation of <see cref="IClob"/> over a <see cref="String"/> object.
	/// </summary>
	class DbClob : IClob {
		/// <summary>
		/// The string the IClob is based on.
		/// </summary>
		private readonly String str;

		public DbClob(String str) {
			this.str = str;
		}

		// ---------- Implemented from IClob ----------

		/// <inheritdoc/>
		public long Length {
			get { return str.Length; }
		}

		/// <inheritdoc/>
		public String Substring(long pos, int length) {
			int p = (int)(pos - 1);
			return str.Substring(p, length);
		}

		/// <inheritdoc/>
		public TextReader CharacterStream {
			get { return new StringReader(str); }
		}

		/// <inheritdoc/>
		public Stream AsciiStream {
			get {
				//TODO: check this...
				byte[] buffer = Encoding.ASCII.GetBytes(str);
				return new MemoryStream(buffer);
			}
		}

		/// <inheritdoc/>
		public long IndexOf(String searchstr, long start) {
			throw DbDataException.Unsupported();
		}

		/// <inheritdoc/>
		public long IndexOf(IClob searchstr, long start) {
			throw DbDataException.Unsupported();
		}
	}
}