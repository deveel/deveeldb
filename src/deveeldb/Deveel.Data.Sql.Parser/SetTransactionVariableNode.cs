using System;
using System.Linq;

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

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			if (VariableName.Equals(IsolationLevelVariable)) {
				var isolationLevel = ParseIsolationLevel(Value);
				builder.AddObject(new SetIsolationLevelStatement(isolationLevel));
			} else if (VariableName.Equals(AccessVariable)) {
				bool status;
				if (String.Equals(Value, "READ ONLY", StringComparison.OrdinalIgnoreCase)) {
					status = true;
				} else if (String.Equals(Value, "READ WRITE", StringComparison.OrdinalIgnoreCase)) {
					status = false;
				} else {
					throw new SqlParseException("Invalid access type");
				}

				builder.AddObject(new SetReadOnlyStatement(status));
			}
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
