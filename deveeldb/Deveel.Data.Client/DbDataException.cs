//  
//  DbDataException.cs
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