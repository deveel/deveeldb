// 
//  Copyright 2010  Deveel
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