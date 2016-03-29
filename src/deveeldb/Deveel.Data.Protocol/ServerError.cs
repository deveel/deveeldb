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

namespace Deveel.Data.Protocol {
	public sealed class ServerError {
		public ServerError(int errorClass, int errorCode, string errorMessage) {
			ErrorMessage = errorMessage;
			ErrorCode = errorCode;
			ErrorClass = errorClass;
		}

		public int ErrorClass { get; private set; }

		public int ErrorCode { get; private set; }

		public string ErrorMessage { get; private set; }
	}
}