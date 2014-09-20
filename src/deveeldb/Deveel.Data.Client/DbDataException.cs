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
using System.Data;
using System.IO;

namespace Deveel.Data.Client {
	///<summary>
	/// A <see cref="DataException"/> used by the database engine.
	///</summary>
	public class DbDataException : DataException {
		private readonly int error_code;
		private readonly String server_error_msg;
		private readonly String server_stack_trace;


		public DbDataException(string message)
			: base(message) {
		}

		public DbDataException() {
		}

		public DbDataException(String message, String server_error_msg, int error_code, Exception server_error)
			: base(message, null) {
			this.error_code = error_code;
			this.server_error_msg = server_error_msg;
			if (server_error != null) {
				StringWriter writer = new StringWriter();
				writer.WriteLine(server_error.StackTrace);
				server_stack_trace = writer.ToString();
			} else {
				server_stack_trace = "<< NO SERVER STACK TRACE >>";
			}
		}

		public DbDataException(String message, String server_error_msg, int error_code, String server_error_trace)
			: base(message, null) {
			this.error_code = error_code;
			this.server_error_msg = server_error_msg;
			this.server_stack_trace = server_error_trace;
		}

		/// <summary>
		/// Returns the error message that generated this exception.
		/// </summary>
		public string ServerErrorMessage {
			get { return server_error_msg; }
		}

		/// <summary>
		/// Returns the server side stack trace for this error.
		/// </summary>
		public string ServerErrorStackTrace {
			get { return server_stack_trace; }
		}

		public int ErrorCode {
			get { return error_code; }
		}

		///<summary>
		/// Returns a <see cref="DataException"/> that is used for all unsupported 
		/// features of the driver.
		///</summary>
		///<returns></returns>
		public static DataException Unsupported() {
			return new DbDataException("Not Supported");
		}

	}
}