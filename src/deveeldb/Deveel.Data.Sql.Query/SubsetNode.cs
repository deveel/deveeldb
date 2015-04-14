using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class SubsetNode : SingleQueryPlanNode {
		public SubsetNode(IQueryPlanNode child, ObjectName[] originalColumnNames, ObjectName[] aliasColumnNames) 
			: base(child) {
			OriginalColumnNames = originalColumnNames;
			AliasColumnNames = aliasColumnNames;
		}

		public ObjectName[] OriginalColumnNames { get; private set; }

		public ObjectName[] AliasColumnNames { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var table = Child.Evaluate(context);
			var columnMap = new int[OriginalColumnNames.Length];

			for (int i = 0; i < columnMap.Length; i++) {
				columnMap[i] = table.IndexOfColumn(OriginalColumnNames[i]);
			}

			return new SubsetColumnTable(table, columnMap, AliasColumnNames);
		}

		public void SetName(ObjectName parentName) {
			var aliases = new ObjectName[AliasColumnNames.Length];
			for (int i = 0; i < aliases.Length; i++) {
				aliases[i] = new ObjectName(parentName, aliases[i].Name);
			}

			AliasColumnNames = aliases;
		}
	}
}
