using System;

namespace Deveel.Data.Linq {
	interface IAssociationConfiguration {
		DbAssociationModel CreateModel(DbModelBuildContext context);
	}
}
