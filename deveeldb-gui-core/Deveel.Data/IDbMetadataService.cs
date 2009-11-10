using System;
using System.Collections;

using Deveel.Data.DbModel;

namespace Deveel.Data {
	public interface IDbMetadataService {
		DbDatabase GetMetadata(string connectionString);

		IDictionary GetDbTypes(string connectionString);
	}
}