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
using System.Linq;

using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	[Serializable]
	public class ErrorException : ApplicationException {
		public ErrorException(int eventClass, int errorCode)
			: this(eventClass, errorCode, null) {
		}

		public ErrorException(int eventClass, int errorCode, string message)
			: this(eventClass, errorCode, message, null) {
		}

		public ErrorException(int eventClass, int errorCode, string message, Exception innerException)
			: base(message, innerException) {
			ErrorCode = errorCode;
			EventClass = eventClass;
		}

		public int EventClass { get; private set; }

		public int ErrorCode { get; private set; }

		protected virtual ErrorLevel ErrorLevel {
			get { return ErrorLevel.Error; }
		}

		public ErrorEvent AsEvent(string databaseName, string userName) {
			IDictionary<string, object> data = new Dictionary<string, object>();
			foreach (DictionaryEntry entry in Data) {
				data[entry.Key.ToString()] = entry.Value;
			}

			data["StackTrace"] = StackTrace;
			data["Source"] = Source;

			return new ErrorEvent(databaseName, userName, EventClass, ErrorCode, ErrorLevel.Error, Message, data);
		}
	}
}