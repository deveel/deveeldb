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

namespace Deveel.Data.Procedures {
	///<summary>
	/// An exception that is generated from a stored procedure when some 
	/// erronious condition occurs.
	///</summary>
	/// <remarks>
	/// This error is typically returned back to the client.
	/// </remarks>
	public class ProcedureException : Exception {
		///<summary>
		///</summary>
		///<param name="message"></param>
		/// <param name="innerException"></param>
		public ProcedureException(string message, Exception innerException)
			: base(message, innerException) {
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="message"></param>
		public ProcedureException(string message)
			: base(message) {
		}
	}
}