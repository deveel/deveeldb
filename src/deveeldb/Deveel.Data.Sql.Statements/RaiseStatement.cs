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

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class RaiseStatement : SqlStatement {
		public RaiseStatement() 
			: this((string) null) {
		}

		public RaiseStatement(string exceptionName) {
			ExceptionName = exceptionName;
		}

		private RaiseStatement(ObjectData data) {
			ExceptionName = data.GetString("ExceptionName");
		}

		public string ExceptionName { get; set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("ExceptionName", ExceptionName);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			throw new NotImplementedException();
		}
	}
}
