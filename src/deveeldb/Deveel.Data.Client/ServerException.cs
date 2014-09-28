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
using System.Runtime.Serialization;

namespace Deveel.Data.Client {
	[Serializable]
	public class ServerException : DataException {
		public ServerException(string message, int errorClass, int errorCode)
			: base(message) {
			ErrorClass = errorClass;
			ErrorCode = errorCode;
		}

		protected ServerException(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ErrorClass = info.GetInt32("ErrorClass");
			ErrorCode = info.GetInt32("ErrorCode");
		}

		public int ErrorClass { get; private set; }

		public int ErrorCode { get; private set; }

		public override void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("ErrorClass", ErrorClass);
			info.AddValue("ErrorCode", ErrorCode);
			base.GetObjectData(info, context);
		}
	}
}