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

		public Encoding Encoding {
			get { return Encoding.Unicode; }
		}

		// ---------- Implemented from IClob ----------

		/// <inheritdoc/>
		public long Length {
			get { return str.Length; }
		}

		/// <inheritdoc/>
		public String GetString(long pos, int length) {
			return str.Substring((int)pos, length);
		}

		/// <inheritdoc/>
		public TextReader GetReader() {
			return new StringReader(str);
		}

		/// <inheritdoc/>
		public Stream GetStream() {
			//TODO: check this...
			byte[] buffer = Encoding.GetBytes(str);
			return new MemoryStream(buffer);
		}

		/// <inheritdoc/>
		public long GetPosition(String searchstr, long start) {
			return str.IndexOf(searchstr, (int)start);
		}

		/// <inheritdoc/>
		public long GetPosition(IClob searchstr, long start) {
			if (!(searchstr is DbClob))
				throw new ArgumentException();

			return GetPosition(((DbClob) searchstr).str, start);
		}
	}
}