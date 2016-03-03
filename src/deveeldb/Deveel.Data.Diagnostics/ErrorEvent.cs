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
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public class ErrorEvent : Event {
		public ErrorEvent(Exception error, int errorCode, ErrorLevel level) {
			if (error == null)
				throw new ArgumentNullException("error");

			Error = error;
			ErrorCode = errorCode;
			Level = level;
		}

		public Exception Error { get; private set; }

		public int ErrorCode { get; private set; }

		public ErrorLevel Level { get; private set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data["error.code"] = ErrorCode;
			data["error.level"] = Level.ToString().ToLowerInvariant();
			data["error.message"] = Error.Message;
			data["error.stackTrace"] = Error.StackTrace;
		}
	}
}
