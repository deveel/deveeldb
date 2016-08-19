using System;
using System.Linq;

namespace Deveel.Data.Linq {
	public interface IDbTable<T> : IQueryable<T>, IDisposable where T : class {
		T FindByKey(object key);
	}
}
