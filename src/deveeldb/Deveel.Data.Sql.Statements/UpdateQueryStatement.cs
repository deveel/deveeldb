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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class UpdateQueryStatement : SqlStatement {
		public UpdateQueryStatement(string tableName, SqlQueryExpression sourceExpression, SqlExpression whereExpression) {
			TableName = tableName;
			SourceExpression = sourceExpression;
			WhereExpression = whereExpression;
		}

		public string TableName { get; private set; }

		public SqlExpression WhereExpression { get; private set; }

		public SqlQueryExpression SourceExpression { get; private set; }

		public int Limit { get; set; }

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			private Prepared(SerializationInfo info, StreamingContext context) {
				
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				throw new NotImplementedException();
			}

			protected override void GetData(SerializationInfo info) {
				base.GetData(info);
			}
		}

		#endregion
	}
}
