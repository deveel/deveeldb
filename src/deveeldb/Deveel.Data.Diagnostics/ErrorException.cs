// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// The base class of all the exceptions handled by the system and that
	/// can be converted to events sent to the diagnostics.
	/// </summary>
	public class ErrorException : Exception {
		public ErrorException(int errorCode)
			: this(errorCode, null) {
		}

		public ErrorException(int errorCode, string message)
			: this(errorCode, message, null) {
		}

		public ErrorException()
			: this(null) {
		}

		public ErrorException(string message)
			: this(message, null) {
		}

		public ErrorException(string message, Exception innerException)
			: this(SystemErrorCodes.Unknown, message, innerException) {
		}

		public ErrorException(int errorCode, string message, Exception innerException)
			: base(message, innerException) {
			ErrorCode = errorCode;
		}

		/// <summary>
		/// Gets a numeric value representing the code of the error catched
		/// by this exception.
		/// </summary>
		public int ErrorCode { get; private set; }

		/// <summary>
		/// Gets the error level of this exception.
		/// </summary>
		protected virtual ErrorLevel ErrorLevel {
			get { return ErrorLevel.Error; }
		}

		/// <summary>
		/// Transforms the error to an event to be passed to the diagnostics,
		/// given a source where this was generated.
		/// </summary>
		/// <param name="source">The source of the error.</param>
		/// <returns>
		/// Returns an instance of <see cref="ErrorEvent"/> that encapsulates all the information
		/// about this exception, that can be routed to the diagnostics.
		/// </returns>
		public ErrorEvent AsEvent(IEventSource source) {
			var e = new ErrorEvent(this, ErrorCode, ErrorLevel);

			if (source != null)
				e.EventSource = source;

			return e;
		}
	}
}