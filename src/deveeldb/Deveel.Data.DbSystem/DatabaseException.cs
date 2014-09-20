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

namespace Deveel.Data.DbSystem {
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