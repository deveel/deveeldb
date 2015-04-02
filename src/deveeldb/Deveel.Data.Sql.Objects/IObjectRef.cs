using System;

using Deveel.Data.Store;

namespace Deveel.Data.Sql.Objects {
	public interface IObjectRef {
		ObjectId ObjectId { get; }
	}
}
