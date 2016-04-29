using System;
using System.Linq;

namespace Deveel.Data.Linq {
	public interface IEntityTable : IQueryable {
		string TableName { get; }

		new IEntityProvider Provider { get; }

		object Find(object key);

		int Insert(object entity);

		int Update(object entity);

		int Delete(object entity);
	}
}
