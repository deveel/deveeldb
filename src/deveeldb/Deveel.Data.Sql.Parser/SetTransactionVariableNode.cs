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
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Parser {
	class SetTransactionVariableNode : SqlStatementNode {
		public string VariableName { get; private set; }

		public string Value { get; private set; }

		private const string IsolationLevelVariable = "IsolationLevel";
		private const string AccessVariable = "Access";

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("isolation_level")) {
				VariableName = IsolationLevelVariable;
				GetIsolationLevel(node);
			} else if (node.NodeName.Equals("access")) {
				VariableName = AccessVariable;
				GetReadAccess(node);
			}

			return base.OnChildNode(node);
		}

		private void GetReadAccess(ISqlNode node) {
			var accessType = node.FindByName("access_type");
			if (accessType == null)
				throw new SqlParseException("Cannot find the read access type.");

			var keys = accessType.ChildNodes.OfType<SqlKeyNode>().Select(x => x.Text).ToArray();
			Value = String.Join(" ", keys);
		}

		private void GetIsolationLevel(ISqlNode node) {
			var levelType = node.FindByName("level_type");
			if (levelType == null)
				throw new SqlParseException("Cannot find the isolation level");

			var keys = levelType.ChildNodes.OfType<SqlKeyNode>().Select(x => x.Text).ToArray();
			Value = String.Join(" ", keys);
		}

		protected override void BuildStatement(SqlStatementBuilder builder) {
			string key;
			object value;

			if (VariableName.Equals(IsolationLevelVariable)) {
				var isolationLevel = ParseIsolationLevel(Value);
				key = TransactionSettingKeys.IsolationLevel;
				value = isolationLevel.ToString();
			} else if (VariableName.Equals(AccessVariable)) {
				bool status;
				if (String.Equals(Value, "READ ONLY", StringComparison.OrdinalIgnoreCase)) {
					status = true;
				} else if (String.Equals(Value, "READ WRITE", StringComparison.OrdinalIgnoreCase)) {
					status = false;
				} else {
					throw new SqlParseException("Invalid access type");
				}

				key = TransactionSettingKeys.ReadOnly;
				value = status;
			} else {
				throw new NotSupportedException(String.Format("Transaction variable '{0}' is not supported.", VariableName));
			}
			
			builder.AddObject(new SetStatement(key, SqlExpression.Constant(value)));			
		}

		private IsolationLevel ParseIsolationLevel(string value) {
			try {
				var s = value.Replace(" ", "");
				return (IsolationLevel) Enum.Parse(typeof (IsolationLevel), s, true);
			} catch (Exception ex) {
				throw new SqlParseException(String.Format("The string '{0}' is not a valid isolation level.", value), ex);
			}
		}
	}
}
