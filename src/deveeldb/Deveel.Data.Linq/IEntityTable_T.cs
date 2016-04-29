using System;

namespace Deveel.Data.Linq {
	public interface IEntityTable<TEntity> : IEntityTable where TEntity : class {
		new TEntity Find(object key);

		int Insert(TEntity entity);

		int Delete(TEntity entity);

		int Update(TEntity entity);
	}
}
