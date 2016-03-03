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

using Deveel.Data;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	/// <summary>
	/// The node for merging the child node with a set of new function 
	/// columns over the entire result.
	/// </summary>
	/// <remarks>
	/// For example, we may want to add an expression <c>a + 10</c> or 
	/// <c>coalesce(a, b, 1)</c>.
	/// </remarks>
	[Serializable]
	class CreateFunctionsNode : SingleQueryPlanNode {
		public CreateFunctionsNode(IQueryPlanNode child, SqlExpression[] functionList, string[] nameList)
			: base(child) {
			Functions = functionList;
			Names = nameList;
		}

		private CreateFunctionsNode(ObjectData data)
			: base(data) {
			Functions = data.GetValue<SqlExpression[]>("Functions");
			Names = data.GetValue<string[]>("Names");
		}

		/// <summary>
		/// The list of functions to create.
		/// </summary>
		public SqlExpression[] Functions { get; private set; }

		/// <summary>
		/// The list of names to give each function table.
		/// </summary>
		public string[] Names { get; private set; }


		public override ITable Evaluate(IRequest context) {
			var childTable = Child.Evaluate(context);
			var funTable = new FunctionTable(childTable, Functions, Names, context);
			return funTable.MergeWith(null);
		}

		protected override void GetData(SerializeData data) {
			data.SetValue("Functions", Functions);
			data.SetValue("Names", Names);
		}
	}
}