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

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DeclareExceptionStatement : SqlStatement, IDeclarationStatement {
		public DeclareExceptionStatement(string exceptionName) {
			if (String.IsNullOrEmpty(exceptionName))
				throw new ArgumentNullException("exceptionName");

			ExceptionName = exceptionName;
		}

		private DeclareExceptionStatement(SerializationInfo info, StreamingContext context) {
			ExceptionName = info.GetString("Exception");
		}

		public string ExceptionName { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Exception", ExceptionName);
		}

		void IDeclarationStatement.AppendDeclarationTo(SqlStringBuilder builder) {
			builder.Append("EXCEPTION ");
			builder.Append(ExceptionName);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("DECLARE ");
			(this as IDeclarationStatement).AppendDeclarationTo(builder);
		}
	}
}