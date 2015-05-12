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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// The base class of all the exceptions handled by the system and that
	/// can be converted to events sent to the diagnostics.
	/// </summary>
#if !PORTABLE
	[Serializable]
#endif
	public class ErrorException : ApplicationException {
		/// <summary>
		/// Constructs a new exception with the given event class and code and
		/// without any specific error message.
		/// </summary>
		/// <param name="eventClass">The class of the event that this error belongs to.</param>
		/// <param name="errorCode">The error specific code.</param>
		public ErrorException(int eventClass, int errorCode)
			: this(eventClass, errorCode, null) {
		}

		/// <summary>
		/// Constructs a new exception with the given event class and code and
		/// with a specific error message.
		/// </summary>
		/// <param name="eventClass">The class of the event that this error belongs to.</param>
		/// <param name="errorCode">The error specific code.</param>
		/// <param name="message">A descriptive error message of the error.</param>
		public ErrorException(int eventClass, int errorCode, string message)
			: this(eventClass, errorCode, message, null) {
		}

		public ErrorException(int eventClass, int errorCode, string message, Exception innerException)
			: base(message, innerException) {
			ErrorCode = errorCode;
			EventClass = eventClass;
		}

		/// <summary>
		/// Gets a numeric value representing the class of the event fired
		/// </summary>
		public int EventClass { get; private set; }

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
			var e = new ErrorEvent(EventClass, ErrorCode, Message);

			foreach (DictionaryEntry entry in Data) {
				e.SetData(entry.Key.ToString(), entry.Value);
			}

			if (source != null)
				source.FillEventData(e);

			e.ErrorLevel(ErrorLevel);
			e.StackTrace(StackTrace);
			e.ErrorSource(Source);

			return e;
		}
	}
}