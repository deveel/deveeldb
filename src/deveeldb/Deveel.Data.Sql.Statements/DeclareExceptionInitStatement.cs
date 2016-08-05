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
using System.Runtime.Serialization;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DeclareExceptionInitStatement : SqlStatement, IDeclarationStatement {
		public DeclareExceptionInitStatement(string exceptionName, int errorCode) {
			if (String.IsNullOrEmpty(exceptionName))
				throw new ArgumentNullException("exceptionName");

			ExceptionName = exceptionName;
			ErrorCode = errorCode;
		}

		private DeclareExceptionInitStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ExceptionName = info.GetString("ExceptionName");
			ErrorCode = info.GetInt32("ErrorCode");
		}

		public string ExceptionName { get; private set; }

		public int ErrorCode { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			context.Request.Context.DeclareException(ErrorCode, ExceptionName);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("ExceptionName", ExceptionName);
			info.AddValue("ErrorCode", ErrorCode);
		}
	}
}
