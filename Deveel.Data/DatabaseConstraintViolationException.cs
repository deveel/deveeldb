// 
//  DatabaseConstraintViolationException.cs
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
	///  A database exception that represents a constraint violation.
	/// </summary>
	public class DatabaseConstraintViolationException : ApplicationException {

		// ---------- Statics ----------

		/// <summary>
		/// A Primary Key constraint violation error code.
		/// </summary>
		public const int PrimaryKeyViolation = 20;

		/// <summary>
		/// A Unique constraint violation error code.
		/// </summary>
		public const int UniqueViolation = 21;

		/// <summary>
		/// A Check constraint violation error code.
		/// </summary>
		public const int CheckViolation = 22;

		/// <summary>
		/// A Foreign Key constraint violation error code.
		/// </summary>
		public const int ForeignKeyViolation = 23;

		/// <summary>
		/// A Nullable constraint violation error code (data added to not null
		/// columns that was null).
		/// </summary>
		public const int NullableViolation = 24;

		/// <summary>
		/// Type constraint violation error code (tried to insert an object
		/// that wasn't derived from the object type defined for the column).
		/// </summary>
		public const int ObjectTypeViolation = 25;

		/// <summary>
		/// Tried to drop a table that is referenced by another source.
		/// </summary>
		public const int DropTableViolation = 26;

		/// <summary>
		/// Column can't be dropped before of an reference to it.
		/// </summary>
		public const int DropColumnViolation = 27;


		/// <summary>
		/// The error code.
		/// </summary>
		private readonly int error_code;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="err_code"></param>
		/// <param name="msg"></param>
		public DatabaseConstraintViolationException(int err_code, String msg)
			: base(msg) {
			error_code = err_code;
		}

		/// <summary>
		/// Returns the violation error code.
		/// </summary>
		public int ErrorCode {
			get { return error_code; }
		}
	}
}