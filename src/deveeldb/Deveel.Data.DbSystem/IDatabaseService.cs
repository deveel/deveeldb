using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.DbSystem {
	public interface IDatabaseService {
		void Configure(IDbConfig config);
	}
}
