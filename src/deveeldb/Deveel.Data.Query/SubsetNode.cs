using System;
using System.Text;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Query {
	/// <summary>
	/// The node for finding a subset and renaming the columns of the 
	/// results in the child node.
	/// </summary>
	[Serializable]
	public class SubsetNode : SingleQueryPlanNode {
		/// <summary>
		/// The original columns in the child that we are to make the subset of.
		/// </summary>
		private readonly VariableName[] originalColumns;

		/// <summary>
		/// New names to assign the columns.
		/// </summary>
		private readonly VariableName[] newColumnNames;


		public SubsetNode(IQueryPlanNode child, VariableName[] originalColumns, VariableName[] newColumnNames)
			: base(child) {
			this.originalColumns = originalColumns;
			this.newColumnNames = newColumnNames;

		}

		public override Table Evaluate(IQueryContext context) {
			Table t = Child.Evaluate(context);

			int sz = originalColumns.Length;
			int[] colMap = new int[sz];

			for (int i = 0; i < sz; ++i) {
				colMap[i] = t.FindFieldName(originalColumns[i]);
			}

			SubsetColumnTable colTable = new SubsetColumnTable(t);
			colTable.SetColumnMap(colMap, newColumnNames);

			return colTable;
		}

		// ---------- Set methods ----------

		/// <summary>
		/// Sets the given table name of the resultant table.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// This is intended if we want to create a sub-query that has an 
		/// aliased table name.
		/// </remarks>
		public void SetGivenName(TableName name) {
			if (name != null) {
				int sz = newColumnNames.Length;
				for (int i = 0; i < sz; ++i) {
					newColumnNames[i].TableName = name;
				}
			}
		}

		// ---------- Get methods ----------

		/// <summary>
		/// Returns the list of original columns that represent the mappings from
		/// the columns in this subset.
		/// </summary>
		public VariableName[] OriginalColumns {
			get { return originalColumns; }
		}

		/// <summary>
		/// Returns the list of new column names that represent the new 
		/// columns in this subset.
		/// </summary>
		public VariableName[] NewColumnNames {
			get { return newColumnNames; }
		}

		public override Object Clone() {
			SubsetNode node = (SubsetNode)base.Clone();
			QueryPlanUtil.CloneArray(node.originalColumns);
			QueryPlanUtil.CloneArray(node.newColumnNames);
			return node;
		}

		public override string Title {
			get {
				StringBuilder sb = new StringBuilder();
				sb.Append("SUBSET: ");
				for (int i = 0; i < newColumnNames.Length; ++i) {
					sb.Append(newColumnNames[i]);
					sb.Append("->");
					sb.Append(originalColumns[i]);
					sb.Append(", ");
				}
				return sb.ToString();
			}
		}
	}
}