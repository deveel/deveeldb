// 
//  Copyright 2010-2015 Deveel
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
//

using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// Exception thrown where various problems occur within the database.
	/// </summary>
	public class DatabaseSystemException : ErrorException {
		/// <summary>
		/// 
		/// </summary>
		/// <param name="errorCode"></param>
		public DatabaseSystemException(int errorCode)
			: this(errorCode, null) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="errorCode"></param>
		/// <param name="message"></param>
		public DatabaseSystemException(int errorCode, string message)
			: this(errorCode, message, null) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="message"></param>
		public DatabaseSystemException(string message)
			: this(message, null) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="message"></param>
		/// <param name="innerException"></param>
		public DatabaseSystemException(string message, Exception innerException)
			: this(SystemErrorCodes.Unknown, message, innerException) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="errorCode"></param>
		/// <param name="message"></param>
		/// <param name="innerException"></param>
		public DatabaseSystemException(int errorCode, string message, Exception innerException)
			: base(errorCode, message, innerException) {
		}
	}
}