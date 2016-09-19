using System;

namespace Deveel.Data.Linq {
	public interface IUpdateQueryTable : IQueryTable {
		object Insert(object obj);

		bool Update(object obj);

		bool Delete(object obj);
	}
}
