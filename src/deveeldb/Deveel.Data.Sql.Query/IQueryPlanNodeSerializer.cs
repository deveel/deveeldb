using System;
using System.IO;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Query {
	public interface IQueryPlanNodeSerializer : IObjectBinarySerializer {
		bool CanSerialize(Type nodeType);
	}
}
