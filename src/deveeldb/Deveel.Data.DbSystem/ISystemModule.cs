using System;

namespace Deveel.Data.DbSystem {
	public interface ISystemModule {
		void Register(ISystemContext context);
	}
}
