using System;
using System.Linq;

namespace Deveel.Data.Linq {
	public interface IQueryTable<TType> : IQueryTable, IQueryable<TType> where TType : class {
		new TType FindByKey(object key);
	}
}
