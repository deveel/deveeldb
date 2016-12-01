using System;
using System.Linq;

namespace Deveel.Data.Linq {
	public interface IQueryTable : IQueryable, IDisposable {
		Type Type { get; }

		object FindByKey(object key);
	}
}
