using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class GroupNode : SingleQueryPlanNode {
		public GroupNode(IQueryPlanNode child, ObjectName groupMaxColumn, SqlExpression[] functions, string[] names) 
			: this(child, new ObjectName[0], groupMaxColumn, functions, names) {
		}

		public GroupNode(IQueryPlanNode child, ObjectName[] columnNames, ObjectName groupMaxColumn, SqlExpression[] functions, string[] names) : base(child) {
			ColumnNames = columnNames;
			GroupMaxColumn = groupMaxColumn;
			Functions = functions;
			Names = names;
		}

		public ObjectName[] ColumnNames { get; private set; }

		public ObjectName GroupMaxColumn { get; private set; }

		public SqlExpression[] Functions { get; private set; }

		public string[] Names { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var childTable = Child.Evaluate(context);
			var funTable = new FunctionTable(childTable, Functions, Names, context);

			// If no columns then it is implied the whole table is the group.
			if (ColumnNames == null) {
				funTable = funTable.AsGroup();
			} else {
				funTable = funTable.CreateGroupMatrix(ColumnNames);
			}

			return funTable.MergeWith(GroupMaxColumn);
		}
	}
}
