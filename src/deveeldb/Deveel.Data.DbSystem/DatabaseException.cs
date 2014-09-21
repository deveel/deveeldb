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
using System.Runtime.Serialization;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Exception thrown where various problems occur within the database.
	/// </summary>
	[Serializable]
	public class DatabaseException : ApplicationException {
		/// <summary>
		/// 
		/// </summary>
		/// <param name="message"></param>
		/// <param name="errorCode"></param>
		public DatabaseException(string message, int errorCode)
			: base(message) {
			ErrorCode = errorCode;
		}

		/// <summary>
		/// 
		/// </summary>
		public DatabaseException()
			: this(null) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="message"></param>
		public DatabaseException(string message)
			: this(message, null) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="message"></param>
		/// <param name="innerException"></param>
		public DatabaseException(string message, Exception innerException)
			: base(message, innerException) {
			ErrorCode = -1;
		}

		protected DatabaseException(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ErrorCode = info.GetInt32("ErrorCode");
		}

		/// <summary>
		/// Returns the error code, or -1 if no error code was given.
		/// </summary>
		public int ErrorCode { get; private set; }

		public override void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("ErrorCode", ErrorCode);
			base.GetObjectData(info, context);
		}
	}
}