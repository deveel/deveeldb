using System;

namespace Deveel.Data.Linq {
	public interface IEntityProvider {
		IEntityTable Table(Type entityType, string tableName);

		IEntityTable<TEntity> Table<TEntity>(string tableName) where TEntity : class;
	}
}
