// 
//  DatabaseException.cs
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

namespace Deveel.Data {
	/// <summary>
	/// Exception thrown where various problems occur within the database.
	/// </summary>
	public class DatabaseException : Exception {

		private readonly int error_code;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="error_code"></param>
		/// <param name="message"></param>
		public DatabaseException(int error_code, String message)
			: base(message) {
			this.error_code = error_code;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="message"></param>
		public DatabaseException(String message)
			: this(-1, message) {
		}

		/// <summary>
		/// Returns the error code, or -1 if no error code was given.
		/// </summary>
		public int ErrorCode {
			get { return error_code; }
		}
	}
}