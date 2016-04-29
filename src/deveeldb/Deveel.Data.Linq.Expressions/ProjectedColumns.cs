using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

using Deveel.Data.Util;

namespace Deveel.Data.Linq.Expressions {
	class ProjectedColumns {
		public ProjectedColumns(Expression projector, IEnumerable<QueryColumn> columns) {
			Projector = projector;
			Columns = columns.ToReadOnly();
		}

		public Expression Projector { get; private set; }

		public ReadOnlyCollection<QueryColumn> Columns { get; private set; }
	}
}
