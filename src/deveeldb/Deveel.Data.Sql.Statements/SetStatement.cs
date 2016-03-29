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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Statements {
	public sealed class SetStatement : SqlStatement {
		public SetStatement(string settingName, SqlExpression valueExpression) {
			if (String.IsNullOrEmpty(settingName))
				throw new ArgumentNullException("settingName");
			if (valueExpression == null)
				throw new ArgumentNullException("valueExpression");

			SettingName = settingName;
			ValueExpression = valueExpression;
		}

		public string SettingName { get; private set; }

		public SqlExpression ValueExpression { get; private set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var value = ValueExpression.Prepare(preparer);
			return new SetStatement(SettingName, value);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			context.Request.Query.Session.Transaction.Context.SetVariable(SettingName, ValueExpression);
		}
	}
}
