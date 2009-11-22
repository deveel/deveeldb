using System;
using System.Collections;
using System.IO;
using System.Text;

namespace Deveel.Data.Select {
	public sealed class SelectExpression {
		private bool distinct;
		private readonly ArrayList columns = new ArrayList();
		private readonly ArrayList groupBy = new ArrayList();
		private readonly ArrayList orderBy = new ArrayList();
		private readonly FromClause fromClause = new FromClause();
		private string whereClause;
		private string havingClause;
		private string groupMaxColumn;
		private CompositeFunction compositeFunction = CompositeFunction.None;  // (None)
		private bool isCompositeAll;
		private SelectExpression nextComposite;

		private readonly ArrayList parameters = new ArrayList();

		private static readonly SelectParser parser = new SelectParser(new StringReader(String.Empty));

		public IList Columns {
			get { return columns; }
		}

		public FromClause From {
			get { return fromClause; }
		}

		public string Where {
			get { return whereClause; }
			set { whereClause = value; }
		}

		public bool Distinct {
			get { return distinct; }
			set { distinct = value; }
		}

		public IList GroupBy {
			get { return groupBy; }
		}

		public string GroupMax {
			get { return groupMaxColumn; }
			set { groupMaxColumn = value; }
		}

		public string Having {
			get { return havingClause; }
			set { havingClause = value; }
		}

		public IList OrderBy {
			get { return orderBy; }
		}

		public bool IsCompositeAll {
			get { return isCompositeAll; }
		}

		public CompositeFunction CompositeFunction {
			get { return compositeFunction; }
		}

		public SelectExpression NextComposite {
			get { return nextComposite; }
		}

		public bool HasParameters {
			get { return parameters.Count > 0; }
		}

		public IList Parameters {
			get { return (IList) parameters.Clone(); }
		}

		public void ChainComposite(SelectExpression expression, CompositeFunction composite, bool isAll) {
			nextComposite = expression;
			compositeFunction = composite;
			isCompositeAll = isAll;
		}

		public override string ToString() {
			return ToString(false);
		}

		public string ToString(bool excludeOrdering) {
			StringBuilder sb = new StringBuilder();
			DumpTo(sb, excludeOrdering);
			return sb.ToString();
		}

		public static SelectExpression Parse(string s) {
			try {
				parser.Reset();
				parser.ReInit(new StringReader(s));
				SelectExpression expression = parser.ParseSelect();
				expression.parameters.AddRange(parser.Parameters);
				return expression;
			} catch(ParseException) {
				//TODO:
				throw new ArgumentException();
			}
		}

		internal void DumpTo(StringBuilder sb, bool excludeOrdering) {
			sb.Append("SELECT ");

			if (distinct) {
				sb.Append("DISTINCT ");
			}

			int colCount = columns.Count;
			for (int i = 0; i < colCount; i++) {
				SelectColumn column = (SelectColumn)columns[i];
				column.DumpTo(sb);

				if (i < colCount - 1)
					sb.Append(", ");
			}

			if (fromClause.Tables.Count > 0) {
				sb.Append(" ");
				fromClause.DumpTo(sb);
			}

			if (whereClause != null && whereClause.Length > 0) {
				sb.Append(" WHERE ");
				sb.Append(whereClause);
			}

			if (groupBy.Count > 0) {
				colCount = groupBy.Count;
				for (int i = 0; i < colCount; i++) {
					ByColumn column = (ByColumn)groupBy[i];
					sb.Append(column.Expression);
					if (i < colCount - 1)
						sb.Append(", ");
				}

				if (groupMaxColumn != null) {
					sb.Append("GROUP MAX ");
					sb.Append(groupMaxColumn);
				}

				if (havingClause != null && havingClause.Length > 0) {
					sb.Append(" HAVING ");
					sb.Append(havingClause);
				}
			}

			if (nextComposite != null) {
				sb.Append(compositeFunction.ToString().ToUpper());
				sb.Append(" ");
				if (isCompositeAll)
					sb.Append("ALL ");

				sb.Append(nextComposite.ToString(true));
			}

			if (!excludeOrdering && orderBy.Count > 0) {
				colCount = orderBy.Count;
				for (int i = 0; i < colCount; i++) {
					ByColumn column = (ByColumn)orderBy[i];
					column.DumpTo(sb);

					if (i < colCount - 1)
						sb.Append(", ");
				}
			}
		}
	}
}