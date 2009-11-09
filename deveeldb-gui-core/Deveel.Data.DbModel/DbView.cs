using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbView : DbObject {
		public DbView(string schema, string name, string selectExpression) 
			: base(schema, name, DbObjectType.View) {
			this.selectExpression = selectExpression;
			columns = new ArrayList();
		}

		private string selectExpression;
		private readonly ArrayList columns;

		public IList Columns {
			get { return (IList) columns.Clone(); }
		}

		public string SelectExpression {
			get { return selectExpression; }
			set { selectExpression = value; }
		}
	}
}